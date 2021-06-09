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

parse_catalog <- function(catalog_list) {

  possible_names <- c("nodes", "sources")

  catalog_names <- possible_names[possible_names %in% names(catalog_list)]

  catalog_names %>%
    purrr::imap_dfr(
      ~ catalog_list %>%
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

docs_catalog <-
  data_dir %>%
    file.path("catalog.json") %>%
    jsonlite::fromJSON()

df_catalog <- parse_catalog(docs_catalog)

# Parse Manifest ----

run_manifest <-
  data_dir %>%
    file.path("manifest.json") %>%
    jsonlite::fromJSON()

names(run_manifest)

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
                    ) %>%
                    list(),
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
          is_enabled = NA,
          materialized_as = NA_character_,
          depends_on = dplyr::tibble(
                        type = NA_character_,
                        unique_id = NA_character_
                        ) %>%
                        dplyr::filter(
                          !is.na(type),
                          !is.na(unique_id)
                        ) %>%
                        list(),
          columns = y[["columns"]] %>%
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
                      ) %>%
                      list(),
          sha256 = NA_character_
        )
  }

  x %>% purrr::map_dfr(read_manifest_source)

}

manifest_sources <-
 run_manifest %>%
    purrr::pluck("sources") %>%
    read_manifest_sources()

manifest_nodes <-
  run_manifest %>%
    purrr::pluck("nodes") %>%
    read_manifest_nodes()

df_manifest <-
  dplyr::bind_rows(
    manifest_nodes,
    manifest_sources
  ) %>%
  dplyr::arrange(
    resource_type,
    database,
    schema,
    name,
    unique_id
  )

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

