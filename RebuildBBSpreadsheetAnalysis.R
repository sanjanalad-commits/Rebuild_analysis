# Load required libraries
library(bigrquery)
library(curl)
library(dplyr)
library(lubridate)
library(httpuv)
library(base)
library(googledrive)
library(stringr)
library(tidyr)
unloadNamespace("plyr")
library(bit64) 
library(bigrquery)

bb <- read.csv('/Users/birdie/Downloads/socal_jurisdictions_2026-02-10.csv', stringsAsFactors = FALSE)


names(bb) <- gsub("\\.", "_",
                  gsub("\\.$", "",
                       gsub("\\.{2,}", ".", names(bb))))

measure <- read.csv('/Users/birdie/Downloads/socal_measures_2026-02-10.csv', stringsAsFactors = FALSE)

names(measure) <- gsub("\\.", "_",
                  gsub("\\.$", "",
                       gsub("\\.{2,}", ".", names(measure))))

merge <- left_join(measure, bb)


project_id <- "slscampaigns-2026"
dataset_id <- "Rebuild_LocalMeasures_2026"
table_id   <- "InitialBBDataExport_010826"

bq_table_upload(
  x = paste(project_id, dataset_id, table_id, sep = "."),
  values = merge,
  write_disposition = "WRITE_TRUNCATE",
  create_disposition = "CREATE_IF_NEEDED"
)


# =============================================================================
# Analysis 1 — Cohort Construction (Baseline Objectives) + DUPLICATE FLAGS
# =============================================================================

# ---- Eligibility filters ----
eligible <- merge %>%
  filter(
    Jurisdiction_Type == "City",
    Total_Population > 10000,
    Ideology_Taxes > 45,
    Measure_Topic == "General Services",
    Measure_Type == "Sales Tax"
  ) %>%
  group_by(Jurisdiction_Name) %>%
  mutate(
    measure_count = n(),
    duplicate_city = measure_count > 1
  ) %>%
  ungroup()

# ---- Cohorts (keep ALL rows; do NOT dedupe) ----
passed <- eligible %>%
  filter(Pass_Fail == "Pass", Measure_Year <= 2018) %>%
  group_by(Jurisdiction_Name) %>%
  mutate(
    cohort_count = n(),
    cohort_duplicate = cohort_count > 1
  ) %>%
  ungroup()

failed <- eligible %>%
  filter(Pass_Fail == "Fail", Measure_Year >= 2020) %>%
  group_by(Jurisdiction_Name) %>%
  mutate(
    cohort_count = n(),
    cohort_duplicate = cohort_count > 1
  ) %>%
  ungroup()

# ---- Normalize party registration counts to percentages ----
add_party_pcts <- function(df) {
  df %>%
    mutate(
      Dem_Pct = Democratic_Registration / Registered_Voters,
      Rep_Pct = Republican_Registration / Registered_Voters,
      NPP_Pct = No_Party_Preference / Registered_Voters,
      AI_Pct  = American_Independent / Registered_Voters
    )
}

passed <- add_party_pcts(passed)
failed <- add_party_pcts(failed)

# Optional cohort sanity check
summary(passed$Dem_Pct)
summary(failed$Dem_Pct)

# =============================================================================
# Analysis 2 — Trend Comparison + Effect Sizes (Extra Analysis)
# NOTE: still uses ALL passed/failed rows (measure-level), not city-level
# =============================================================================

vars <- c(
  "Over_65",
  "Hispanic_Latino_Population",
  "Median_Household_Income",
  "Median_Home_Value",
  "Homeownership_Rate",
  "Public_Assistance_SNAP",
  "Dem_Pct",
  "Rep_Pct",
  "NPP_Pct",
  "AI_Pct",
  "Student_Population",
  "Political_Lean",
  "Harris_24_Margin",
  "Ideology_Rent_Control",
  "Ideology_Social_Issues",
  "Ideology_Criminal_Justice"
)

# ---- Basic comparison (means/medians) ----
results <- data.frame(
  Variable      = vars,
  Passed_Mean   = sapply(passed[vars], mean,   na.rm = TRUE),
  Failed_Mean   = sapply(failed[vars], mean,   na.rm = TRUE),
  Passed_Median = sapply(passed[vars], median, na.rm = TRUE),
  Failed_Median = sapply(failed[vars], median, na.rm = TRUE)
)

results$Mean_Diff   <- results$Passed_Mean - results$Failed_Mean
results$Median_Diff <- results$Passed_Median - results$Failed_Median

# ---- Effect sizes (standardized differences) ----
results$Passed_SD <- sapply(passed[vars], sd, na.rm = TRUE)
results$Failed_SD <- sapply(failed[vars], sd, na.rm = TRUE)
results$Pooled_SD <- sqrt((results$Passed_SD^2 + results$Failed_SD^2) / 2)
results$Effect_Size <- (results$Passed_Mean - results$Failed_Mean) / results$Pooled_SD

results_sorted <- results[order(abs(results$Effect_Size), decreasing = TRUE), ]

top_vars <- results_sorted %>%
  filter(!is.na(Effect_Size)) %>%
  arrange(desc(abs(Effect_Size)))

print(results)
print(results_sorted)

# =============================================================================
# Write outputs to Google Sheet (4 tabs)
# =============================================================================

gs4_auth()
sheet_id <- "1n5dCjxlw_98CQ2iQ0bJlnaROS5Dg1tv5Ozow3qKjwoQ"

write_tab <- function(df, tab_name) {
  existing <- sheet_names(sheet_id)
  if (tab_name %in% existing) sheet_delete(ss = sheet_id, sheet = tab_name)
  sheet_write(df, ss = sheet_id, sheet = tab_name)
}

write_tab(passed,        "Passed Measures (≤2018)")
write_tab(failed,        "Failed Measures (≥2020)")
write_tab(results,       "Pass vs Fail Comparison")
write_tab(top_vars,      "Key Differentiators")
