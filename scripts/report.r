########
# Generate a cleaned CSV report from the HTTP trace
# to track how long it takes to perform each graph operation
#####


library(dplyr)
library(rprojroot)

# Find the directory of the script reliably
script_dir <- rprojroot::thisfile()

if (is.null(script_dir)) {
  script_dir <- getwd()  # Fallback to current working directory if running interactively
} else {
  script_dir <- dirname(script_dir)
}

# Read CSV relative to the script's directory
data <- read.csv(file.path(script_dir, "../internal/synchronizer/http_trace.csv"), stringsAsFactors = FALSE)

# Convert Timestamp to POSIXct (datetime) using ymd_hms
data <- data %>%
  select(-Error) %>%
  select(-Target.Address) %>%
  select(-Connection.Reused)

# Write sorted output file in the same directory
write.csv(data, file.path(script_dir, "full_output.csv"), row.names = FALSE)

# drop all events except GotFirstResponseByte
data <- data %>% filter(Event == "GotFirstResponseByte")

data <- data %>% mutate(
    Graph.Interaction.Percentage = round((Duration..µs. / sum(Duration..µs.)) * 100, 3)
  ) %>%
  relocate(Graph.Interaction.Percentage, .after = Duration..µs.)

write.csv(data, file.path(script_dir, "just_first_response.csv"), row.names = FALSE)