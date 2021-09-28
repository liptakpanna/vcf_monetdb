getEnvVar <- function(var, fallback) {
    value <- Sys.getenv(var)
    if(value == ""){
        return(fallback)
    }
    return(value)
}