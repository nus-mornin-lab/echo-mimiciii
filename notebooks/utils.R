strip_space <- function(s) {
    gsub("[[:space:]]+$", "", s)
}

rm_semi_colon_end <- function(s) {
    space_stripped <- strip_space(s)
    if (endsWith(space_stripped, ";")) return(space_stripped %>% substr(1, nchar(.) - 1))
    return(s)
}

make_view_sql <- function(sql, title, type = "materialized view") {
    sprintf(
        "drop %3$s if exists %2$s cascade;
         create %3$s %2$s as (
             %1$s
         );",
        rm_semi_colon_end(sql), title, type
    )
}

file_to_sql_view <- function(fname, title, type = "materialized view") {
    readr::read_file(fname) %>% make_view_sql(title, type)
}
