rm_semi_colon_end <- function(s) {
    if (endsWith(s, ";")) return(s %>% substr(1, nchar(.) - 1))
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