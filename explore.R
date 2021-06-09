library(tidyverse)

data_dir <- file.path("C:", "dev", "finances", "dbt", "target")

# Parse Catalog ----

read_catalog_objects <- function(obj) {

  obj %>%
    purrr::imap_dfr(
      ~ dplyr::tibble(
            unique_name = .y,
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
            dbt_type = .x
          ) %>%
          dplyr::select(
            dbt_type,
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

manifest_nodes <-
  run_manifest %>%
    purrr::pluck("nodes") %>%
    purrr::map_dfr(
      ~ dplyr::tibble(
          unique_id = .x[["unique_id"]],
          resource_type = .x[["resource_type"]],
          database = .x[["database"]],
          schema = .x[["schema"]],
          name = dplyr::coalesce(.x[["alias"]], .x[["name"]]),
          description = .x[["description"]],
          is_enabled = .x[["config"]][["enabled"]],
          materialized_as = .x[["config"]][["materialized"]],
          depends_on = .x[["depends_on"]] %>%
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
                    .x[["checksum"]][["name"]] == "sha256",
                    .x[["checksum"]][["checksum"]],
                    NA_character_)
      )
    ) %>%
    dplyr::arrange(
      resource_type,
      is_enabled,
      database,
      schema,
      name,
      unique_id
    )

manifest_sources <-
  run_manifest %>%
    purrr::pluck("sources") %>%
    purrr::map_dfr(
      ~ dplyr::tibble(
          unique_id = .x[["unique_id"]],
          resource_type = .x[["resource_type"]],
          database = .x[["database"]],
          schema = .x[["schema"]],
          name = .x[["identifier"]],
          description = .x[["description"]],
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
          columns = .x[["columns"]] %>%
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
    )

dplyr::bind_rows(
  manifest_nodes,
  manifest_sources
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

