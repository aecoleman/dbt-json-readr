library(tidyverse)

data_dir <- file.path("C:", "dev", "finances", "dbt", "target")

# Parse Catalog ----

read_catalog_objects <- function(obj) {

  obj %>%
    purrr::imap_dfr(
      ~ dplyr::tibble(
            unique_id = .y,
            materialized_as = .x[["metadata"]][["type"]],
            database = .x[["metadata"]][["database"]],
            schema = .x[["metadata"]][["schema"]],
            name = .x[["metadata"]][["name"]],
            columns = .x %>%
                        purrr::pluck("columns") %>%
                        purrr::map_dfr(
                          function(x) {
                            dplyr::tibble(
                              column_name = x[["name"]],
                              ordinal_position = x[["index"]],
                              data_type = x[["type"]]
                            )
                          }
                        ) %>%
                        list()
        )
    ) %>%
    dplyr::arrange()

}

import_catalog_json <- function(catalog) {

  if (is.character(catalog) && length(catalog) == 1L && file.exists(catalog)) {
    catalog <- catalog %>% jsonlite::fromJSON()
  } else {
    stopifnot(is.list(catalog))
  }

  possible_names <- c("nodes", "sources")

  catalog_names <- possible_names[possible_names %in% names(catalog)]

  catalog_names %>%
    purrr::imap_dfr(
      ~ catalog %>%
          purrr::pluck(.x) %>%
          read_catalog_objects() %>%
          dplyr::mutate(
            manifest_group = .x
          ) %>%
          dplyr::select(
            unique_id,
            manifest_group,
            database,
            schema,
            name,
            dplyr::everything()
          )
    )

}

df_catalog <-
  data_dir %>%
    file.path("catalog.json") %>%
    import_catalog_json()

# Parse Manifest ----

parse_columns <- function(x) {

  if (length(x) == 0 || is.na(x)) {
    return(
      dplyr::tibble(
        name = NA_character_,
        description = NA_character_,
        data_type = NA_character_,
        meta = list(key = NA_character_),
        tags = list(NA_character_)
      ) %>%
      dplyr::filter(!is.na(name))
    )
  }

  x %>%
    purrr::map_dfr(
      ~ dplyr::tibble(
        name = .x[["name"]],
        description = .x[["description"]],
        data_type = dplyr::if_else(
          is.null(.x[["data_type"]]),
          NA_character_,
          .x[["data_type"]]
        ),
        meta = list(.x[["meta"]]),
        tags = list(.x[["tags"]])
      )
    )
}

parse_depends_on <- function(x) {

  if (length(x) == 0 || is.na(x)) {
    return(
      dplyr::tibble(
        type = NA_character_,
        unique_id = NA_character_
      ) %>%
      dplyr::filter(
        !is.na(type),
        !is.na(unique_id)
      )
    )
  }

  x %>%
    purrr::imap_dfr(
      ~ if (length(.x) > 0) {
          dplyr::tibble(
            type = .y,
            unique_id = .x
          )
        } else {
          dplyr::tibble(
            type = NA_character_,
            unique_id = NA_character_
          ) %>%
            dplyr::filter(
              !is.na(type),
              !is.na(unique_id)
            )
        }
    )
}

read_manifest_nodes <- function(x) {

  read_manifest_node <- function(y) {
    dplyr::tibble(
      unique_id = y[["unique_id"]],
      manifest_group = "nodes",
      resource_type = y[["resource_type"]],
      database = y[["database"]],
      schema = y[["schema"]],
      name = dplyr::coalesce(y[["alias"]], y[["name"]]),
      description = y[["description"]],
      is_enabled = y[["config"]][["enabled"]],
      materialized_as = y[["config"]][["materialized"]],
      depends_on = y[["depends_on"]] %>%
                    parse_depends_on() %>%
                    list(),
      columns = y[["columns"]] %>% parse_columns() %>% list(),
      meta = list(y[["meta"]]),
      tags = list(y[["tags"]]),
      sha256 = dplyr::if_else(
                y[["checksum"]][["name"]] == "sha256",
                y[["checksum"]][["checksum"]],
                NA_character_)
    )
  }

  x %>%
    purrr::map_dfr(read_manifest_node)

}

read_manifest_sources <- function(x) {

  read_manifest_source <- function(y) {
    dplyr::tibble(
          unique_id = y[["unique_id"]],
          manifest_group = "sources",
          resource_type = y[["resource_type"]],
          database = y[["database"]],
          schema = y[["schema"]],
          name = y[["identifier"]],
          description = y[["description"]],
          is_enabled = y[["config"]][["enabled"]],
          materialized_as = NA_character_,
          depends_on = NA %>% parse_depends_on() %>%
                        list(),
          columns = y[["columns"]] %>%
                      parse_columns() %>%
                      list(),
          meta = list(y[["meta"]]),
          tags = list(y[["tags"]]),
          sha256 = NA_character_
        )
  }

  x %>% purrr::map_dfr(read_manifest_source)

}

read_manifest_macros <- function(x) {

  read_manifest_macro <- function(y) {
    dplyr::tibble(
      unique_id = y[["unique_id"]],
      manifest_group = "macros",
      resource_type = y[["resource_type"]],
      database = NA_character_,
      schema = NA_character_,
      name = y[["name"]],
      description = y[["description"]],
      is_enabled = NA,
      materialized_as = NA_character_,
      depends_on = y[["depends_on"]] %>% parse_depends_on() %>% list(),
      columns = list() %>% parse_columns() %>% list(),
      meta = list(y[["meta"]]),
      tags = list(y[["tags"]]),
      sha256 = digest::digest(y[["macro_sql"]], algo = "sha256")
    )
  }

  x %>% purrr::map_dfr(read_manifest_macro)
}

import_manifest_json <- function(manifest) {

  if (is.character(manifest) && length(manifest) == 1 && file.exists(manifest)) {
    manifest <- manifest %>% jsonlite::fromJSON()
  } else {
    stopifnot(is.list(manifest))
  }

  manifest_sources <-
    manifest %>%
    purrr::pluck("sources") %>%
    read_manifest_sources()

  manifest_nodes <-
    manifest %>%
    purrr::pluck("nodes") %>%
    read_manifest_nodes()

  manifest_macros <-
    manifest %>%
    purrr::pluck("macros") %>%
    read_manifest_macros()

  dplyr::bind_rows(
      manifest_nodes,
      manifest_sources,
      manifest_macros
    ) %>%
    dplyr::arrange(
      resource_type,
      database,
      schema,
      name,
      unique_id
    )

}

manifest_path <-
  data_dir %>%
  file.path("manifest.json")

manifest <-
  manifest_path %>%
  jsonlite::fromJSON()

names(manifest)

df_manifest <-
  data_dir %>%
  file.path("manifest.json") %>%
  import_manifest_json()


# Parse Source ----

source_freshness <-
  data_dir %>%
    file.path("sources.json") %>%
    jsonlite::fromJSON()

# Parse Result ----

dbt_result <-
  data_dir %>%
    file.path("run_results.json") %>%
    jsonlite::fromJSON()

