#!/usr/bin/env Rapp
#| name: srg-papers-bot
#| description: Check for new ART-XC or eROSITA papers and send them to Telegram.

suppressMessages(
  {
    library(aRxiv)
    library(tidyverse)
    library(rvest)
    library(progress)
    library(telegram.bot)
    library(httr)
    library(logger)
  }
)

source("helpers.R")

#| description: Search mode (artxc or erosita)
#| required: true
mode <- NULL

mode <- tolower(mode)
checkmate::assert_choice(mode, c("artxc", "erosita"))

CHAT_ID <- 247777582


TOKEN <- Sys.getenv("SRG_ARTXC_PAPER_BOT_TOKEN")

# index 1: консоль + файл
log_appender(appender_tee("result.log"))

# Инициализация бота ------------------------------------------------------

bot <- telegram.bot::Bot(token = TOKEN)
bot$sendChatAction(CHAT_ID,
                   action = "typing")

# index 2: лог текущего запуска (отправится в Telegram одним сообщением в finally)
run_log <- tempfile(fileext = ".log")
log_appender(appender_file(run_log), index = 2)
log_layout(layout_glue_generator(), index = 2)

log_info("--- Start ({mode}) ---")


tryCatch({

  # Параметры, зависящие от mode ---------------------------------------------
  ads_query <- switch(mode,
    artxc   = 'title:"ART-XC" OR abs:"ART-XC"',
    erosita = '(title:"eROSITA" OR abs:"eROSITA") AND (author:"Gilfanov" OR author:"Churazov" OR author:"Sunyaev")'
  )

  arxiv_query1 <- switch(mode,
    artxc   = 'ti:ART AND ti:XC AND cat:astro-ph*',
    erosita = 'ti:eROSITA AND cat:astro-ph*'
  )

  arxiv_query2 <- switch(mode,
    artxc   = 'abs:ART AND abs:XC AND abs:SRG AND cat:astro-ph*',
    erosita = 'abs:eROSITA AND (au:Gilfanov OR au:Churazov OR au:Sunyaev) AND cat:astro-ph*'
  )

  tg_channel <- switch(mode,
    artxc   = "@srg_artxc",
    erosita = "@srg_erosita"
  )

  papers_file <- switch(mode,
    artxc   = "papers_in_telegram.Rds",
    erosita = "papers_in_telegram_ero_ru.Rds"
  )

  # Парсинг публикаций на сайте srg.cosmos.ru/publications/ ------------------
  df_srg_journals <- safely_df_from_api(get_df_from_srg_papers,
                                        time_sleep = 10)

  # Парсинг телеграмм на сайте srg.cosmos.ru/publications/telegrams ----------
  df_srg_telegrams <- safely_df_from_api(get_df_from_srg_telegrams,
                                         time_sleep = 10)

  # Парсинг arXiv ------------------------------------------------------------
  df_arxiv1 <- safely_df_from_api(arxiv_search,
                                  time_sleep = 300,
                                  query = arxiv_query1,
                                  limit = 1e2,
                                  sort_by = 'submitted',
                                  ascending = FALSE)
  Sys.sleep(5)
  df_arxiv2 <- safely_df_from_api(arxiv_search,
                                  time_sleep = 300,
                                  query = arxiv_query2,
                                  limit = 1e2,
                                  sort_by = 'submitted',
                                  ascending = FALSE)

  df_arxiv <- anti_join(df_arxiv1, df_arxiv2, by = 'id') |>
    bind_rows(df_arxiv2) |>
    mutate(arxiv = clear_arxiv_link(link_abstract)) |>
    select(title, arxiv, everything())

  # Парсинг ADSABS -----------------------------------------------------------
  df_adsabs_all <- safely_df_from_api(get_df_from_adsabs, time_sleep = 5,
                                      q = ads_query)

  journal_names <- readRDS("journal_abbreviations")

  df_adsabs_journals <- df_adsabs_all |>
    filter(journal %in% journal_names,
           !grepl("(ATel|GCN)", journal))

  df_adsabs_telegrams <- df_adsabs_all |>
    filter(journal %in% journal_names,
           grepl("(ATel|GCN)", journal)) |>
    mutate(number = substr(bibcode, 5, 19) |> stringr::str_extract("\\d+"),
           link = paste0("https://ui.adsabs.harvard.edu/abs/", bibcode, "/abstract"))

  # Проверка новых публикаций -------------------------------------------------
  right_field <- function(x, y) ifelse(is.na(x), y, x)

  df_full_journals <- df_adsabs_journals |>
    full_join(df_arxiv, by = c("adsabs_arxiv" = "arxiv"), suffix = c("_ads", '_arxiv')) |>
    mutate(link_adsabs = paste0("https://ui.adsabs.harvard.edu/abs/", bibcode, "/abstract")) |>
    rename(link_arxiv = link_abstract) |>
    mutate(link = right_field(link_adsabs, link_arxiv),
           title = right_field(title_ads, title_arxiv),
           authors = right_field(authors_ads, authors_arxiv),
           abstract = right_field(abstract_ads, abstract_arxiv)) |>
    filter(is_rus != "-") |>
    select(link, title, authors, abstract, bibcode, adsabs_arxiv)

  df_new_journals <- df_full_journals |>
    anti_join(df_srg_journals, by = c("bibcode" = "srg_bibcode"))

  df_new_telegrams <- df_adsabs_telegrams |>
    anti_join(df_srg_telegrams, by = "number") |>
    select(link, title, authors, abstract, bibcode, adsabs_arxiv)

  df_new <- bind_rows(df_new_journals, df_new_telegrams)

  log_info("ADSABS journals: {nrow(df_adsabs_journals)}, arXiv: {nrow(df_arxiv)}, telegrams: {nrow(df_adsabs_telegrams)}")
  log_info("New journals: {nrow(df_new_journals)}, new telegrams: {nrow(df_new_telegrams)}")

  # Отправка в Telegram канал ------------------------------------------------

  if (nrow(df_new) > 0) {
    filename <- here::here(papers_file)

    if (file.exists(filename)) {
      papers_in_telegram <- readRDS(filename)

      new_papers <- df_new |>
        anti_join(papers_in_telegram, by = 'title')

      papers_in_telegram <- bind_rows(papers_in_telegram, new_papers)
      saveRDS(papers_in_telegram, filename)

      has_new <- nrow(new_papers) > 0
    } else {
      saveRDS(object = df_new, file = filename)
      has_new <- TRUE
      new_papers <- df_new
    }

    log_info("New papers to send: {nrow(new_papers)}")

    if (has_new) {
      bot$sendChatAction(CHAT_ID, action = "typing")
      Sys.sleep(1)
      papers_for_telegram <- format_paper_for_telegram(
        link    = new_papers$link,
        title   = new_papers$title,
        authors = new_papers$authors
      )

      purrr::iwalk(papers_for_telegram, \(paper, idx) {
        log_info("Sending {idx}/{length(papers_for_telegram)}: {new_papers$title[idx]}")
        bot$send_message(tg_channel,
                         text = paper,
                         parse_mode = "HTML")
        Sys.sleep(1.5)
      })

    } else {
      log_info("No new papers today.")
    }

  }

},
error = function(cond) {
  log_error("{conditionMessage(cond)}")
},
warning = function(cond) {
  log_warn("{conditionMessage(cond)}")
  invokeRestart("muffleWarning")
},
finally = {
  log_info("Done")
  bot$sendMessage(CHAT_ID, text = paste(readLines(run_log), collapse = "\n"))
}
)
