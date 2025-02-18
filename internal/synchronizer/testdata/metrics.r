library(dplyr)
library(lubridate)
library(rprojroot)

# Find the directory of the script reliably
script_dir <- rprojroot::thisfile()

if (is.null(script_dir)) {
  script_dir <- getwd()  # Fallback to current working directory if running interactively
} else {
  script_dir <- dirname(script_dir)
}

# Read CSV relative to the script's directory
data <- read.csv(file.path(script_dir, "../http_trace.csv"), stringsAsFactors = FALSE)

# Ensure correct column names (remove spaces if needed)
colnames(data) <- trimws(colnames(data))

# Convert Timestamp to POSIXct (datetime) using ymd_hms
data <- data %>%
  mutate(
    Timestamp = ymd_hms(Timestamp),  # Parse RFC3339 timestamp
  ) %>%
  arrange(Timestamp)  %>% 
  select(-Error) %>%
  select(-Target.Address) %>%
  select(-Connection.Reused)

# Write sorted output file in the same directory
write.csv(data, file.path(script_dir, "full_output.csv"), row.names = FALSE)

# drop all events except GotFirstResponseByte
data <- data %>% filter(Event == "GotFirstResponseByte")

data <- data %>% mutate(
    Graph.Interaction.Percentage = round((Duration..µs. / sum(Duration..µs.)) * 100, 3)
  )

write.csv(data, file.path(script_dir, "just_first_response.csv"), row.names = FALSE)