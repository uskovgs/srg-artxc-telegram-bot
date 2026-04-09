
# Для работы с srg.cosmos.ru/publications ---------------------------------

parse_srg_paper_links <- function(page){
  session <- rvest::session(page)
  
  paper_links <- session |>
    html_elements("li") |>
    html_elements("a") |>
    html_attr("href")
  
  arxiv_link <- paper_links[grepl("arxiv.org/abs", paper_links)] 
  arxiv_link <- if (length(arxiv_link) == 0) NA_character_ else arxiv_link

  adsabs_link <- paper_links[grepl("ui.adsabs.harvard.edu", paper_links)] 
  adsabs_link <- if (length(adsabs_link) == 0) NA_character_ else adsabs_link
  
  tibble(srg_arxiv_url = arxiv_link,
         srg_bibcode = adsabs_link)
}
get_df_from_srg_papers <- function(){
  pgsession <- rvest::session("https://www.srg.cosmos.ru/publications/")
  
  rm_button <- pgsession |>
    html_elements('.btn')
  
  xml2::xml_remove(rm_button)
  
  html_res <- pgsession |>
    html_element('tbody') |>
    html_elements('a')
  
  pb <- progress::progress_bar$new(
    format = "Parsing arxiv urls [:bar] :elapsed",
    total = length(html_res))
  
  tibble(title_srg = html_res |> html_elements('strong') |> html_text(),
         page_srg = html_res |> html_attr("href")) |>
    mutate(
      page_srg = paste0("https://www.srg.cosmos.ru", page_srg),
      df = purrr::map(page_srg, \(x) {
        pb$tick()
        parse_srg_paper_links(x)
      }
      )
    ) |>
    unnest_wider(df) |>
    unnest(srg_arxiv_url) |>
    unnest(srg_bibcode) |>
    mutate(srg_arxiv = clear_arxiv_link(srg_arxiv_url),
           srg_bibcode = gsub("https?://ui.adsabs.harvard.edu/abs/", "", x = srg_bibcode))
}


# Для работы с srg.cosmos.ru/publications/telegrams -----------------------

# https://www.srg.cosmos.ru/publications/telegrams/gcn
# https://www.srg.cosmos.ru/publications/telegrams/atel

get_df_from_srg_telegrams <- function(){
  pgsession <- rvest::session("https://www.srg.cosmos.ru/publications/telegrams/atel")
  
  atel_names <- pgsession |>
    html_element('tbody') |>
    html_elements('tr') |> 
    html_elements('td') |> 
    html_element("b")
  atel_names <- atel_names[grepl("ATEL", atel_names)] |>
    html_text()

  atel_numbers <- stringr::str_extract(atel_names, "\\d+")
  
  pgsession <- session_jump_to(pgsession, "https://www.srg.cosmos.ru/publications/telegrams/gcn")
  
  gcn_names <- pgsession |>
    html_element('tbody') |>
    html_elements('tr') |>
    html_elements('td') |>
    html_element("b")
  gcn_names <- gcn_names[grepl("ATEL", gcn_names)] |> 
    html_text()
  gcn_numbers <- stringr::str_extract(gcn_names, "\\d+")
  
  
  tibble(
    type = c(rep("ATEL", length(atel_numbers)), rep("GCN", length(gcn_names))),
    number = c(atel_numbers, gcn_numbers)
  )
  
}

# pgsession <- rvest::session("https://www.srg.cosmos.ru/publications/telegrams/atel")

# pgsession %>% 
#   html_element('tbody') %>% 
#   html_elements('tr') %>% 
#   html_elements('td') %>% 
#   html_element("b") %>% 
#   .[grepl("ATEL", .)]%>% 
#   html_text()

# Для работы с ADSABS API -------------------------------------------------

get_df_from_adsabs <- function(q = 'title:"ART-XC" OR abs:"ART-XC"') {
  query <- list(
    q = q,
    rows = "2000",
    fl = "title,author,abstract,pubdate,date,bibcode,doi,identifier,aff",
    fq = "database:astronomy",
    sort = "date+desc"
  )

  res <- httr::GET("https://api.adsabs.harvard.edu/v1/search/query",
    query = query,
    add_headers(
      "Content-Type" = "application/json",
      "Authorization" = paste("Bearer", Sys.getenv("ADSABS_TOKEN"))
    )
  )

  l <- httr::content(res)$response$docs

  tibble(
    title = purrr::map_chr(l, \(x) x$title[[1]]),
    date = purrr::map_chr(l, \(x) x$date) |> lubridate::ymd_hms(),
    authors = purrr::map_chr(l, \(x) x$author |> unlist() |> paste(collapse = "|")),
    abstract = purrr::map_chr(l, \(x) if(is.null(x$abstract)) NA_character_ else x$abstract),
    bibcode = purrr::map_chr(l, \(x) x$bibcode),
    doi = purrr::map_chr(l, \(x) ifelse(is.null(x$doi[[1]]), NA_character_, x$doi[[1]])),
    adsabs_arxiv = purrr::map(l, \(x){
      identifiers <- x$identifier |> unlist()
      id1 <- identifiers[grepl("arXiv", identifiers)]
      if(length(id1) == 0) NA_character_ else id1
    }),
    is_rus = purrr::map_chr(l, \(x){
      affilations <- x$aff |> unlist()
      
      if(any(grepl('russia', affilations, ignore.case = TRUE)))
        return("+")
      else if(all(affilations == "-"))
        return("?")
      else
        return("-")
    })
  ) |>
    mutate(
      adsabs_arxiv = clear_arxiv_link(adsabs_arxiv),
      journal = substr(bibcode, 5, 9) |> gsub("\\.", "", x = _)
    ) |> 
    arrange(desc(date))
}

# Утилиты -----------------------------------------------------------------

clear_arxiv_link <- function(arxiv_abs_link){
  gsub("v\\d", "", x = arxiv_abs_link) |>
    gsub("https?://arxiv.org/abs/", "", x = _) |>
    gsub("arXiv:", "", x = _)
}

format_paper_for_telegram <- function(link, title, authors){
  title <- title |>
    str_remove_all("\n") |>
    str_squish()
  title <- paste0("<b>", title, "</b>")
  
  authors <- paste0("<i>", authors, "</i>")
  # abstract <- abstract %>% 
  #   str_remove_all("\n") %>% 
  #   # str_replace_all("`", "\\\\`") %>%
  #   # str_replace_all("\\*", "\\\\*") %>%
  #   # str_replace_all("_", "\\\\_") %>%
  #   str_squish()
  paste(title, link, authors, sep = "\n")
}

safely_df_from_api <- function(f, time_sleep = 300, ...) {
  f_name <- substitute(f)
  
  for (i in 1:10) {
    
    df <- f(...) |> as_tibble()
    
    if (nrow(df) > 0) {
      log_info("✅ {f_name}")
      break
    } else {
      log_warn("⚠️ {f_name}: retry in {time_sleep / 60} min.")
      Sys.sleep(time_sleep)
    }
  }
  return(df)
}
