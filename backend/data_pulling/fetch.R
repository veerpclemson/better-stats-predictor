library(nflfastR)
library(tidyverse)
library(lubridate) # for date calculations

SEASONS <- 2021:2025
OUTPUT_DIR <- "../data_pulling/"

# ensure output directory exists
if(!dir.exists(OUTPUT_DIR)) dir.create(OUTPUT_DIR, recursive = TRUE)

# ------------------------
# Fetch play-by-play data
# ------------------------
cat("Fetching play-by-play data for seasons:", SEASONS, "\n")
pbp <- load_pbp(SEASONS)

pbp_small <- pbp %>%
  select(
    game_id, season, week, posteam, defteam, play_type,
    passer_player_id, rusher_player_id, receiver_player_id, air_yards, yards_after_catch,
    rushing_yards, pass_attempt, complete_pass
  ) %>%
  mutate(
    reception = ifelse(complete_pass == 1, 1, 0),
    receiving_yards = air_yards + yards_after_catch
  )

pbp_file <- paste0(OUTPUT_DIR, "pbp_", min(SEASONS), "_", max(SEASONS), ".csv")
write_csv(pbp_small, pbp_file)
cat("Saved play-by-play to", pbp_file, "\n")

# ------------------------
# Fetch roster data
# ------------------------
cat("Fetching roster data\n")
roster <- fast_scraper_roster(SEASONS)

roster_file <- paste0(OUTPUT_DIR, "roster_", min(SEASONS), "_", max(SEASONS), ".csv")
write_csv(roster, roster_file)
cat("Saved roster to", roster_file, "\n")

cat("Fetching game metadata\n")
games <- fast_scraper_schedules(SEASONS)

# select relevant columns for model context
games_context <- games %>%
  select(
    season, week,
    game_id,
    home_team, away_team,
    home_score, away_score,
    
  )
   # optional: ensure date format

# save CSV
games_file <- paste0(OUTPUT_DIR, "games_scores", min(SEASONS), "_", max(SEASONS), ".csv")
write_csv(games_context, games_file)
cat("Saved game context to", games_file, "\n")

cat("Fetching play-by-play data for seasons:", SEASONS, "\n")
pbp <- load_pbp(SEASONS)

# Filter for passing plays only
pbp_pass <- pbp %>% 
  filter(pass_attempt == 1)

# Defensive heuristics
pbp_def <- pbp_pass %>%
  mutate(
    # Blitz heuristic: TFL or sack
    blitz = ifelse(!is.na(tackle_for_loss_1_player_id) | 
                   !is.na(tackle_for_loss_2_player_id) |
                   !is.na(sack_player_id), 1, 0),
    
    # Pressure heuristic: QB hit or sack
    pressure = ifelse(!is.na(qb_hit_1_player_id) |
                      !is.na(qb_hit_2_player_id) |
                      !is.na(sack_player_id), 1, 0),
    
    # Coverage heuristic: approximate man coverage if any defender listed, zone otherwise
    man_coverage = ifelse(!is.na(pass_defense_1_player_id) | 
                          !is.na(pass_defense_2_player_id), 1, 0),
    
    zone_coverage = 1 - man_coverage
  ) %>%
  select(season, week, defteam, blitz, pressure, man_coverage, zone_coverage)

# Aggregate per defense/week/season
def_tendencies <- pbp_def %>%
  group_by(season, week, defteam) %>%
  summarise(
    total_pass_plays = n(),
    blitz_rate = sum(blitz, na.rm = TRUE)/n(),
    pressure_rate = sum(pressure, na.rm = TRUE)/n(),
    man_coverage_pct = sum(man_coverage, na.rm = TRUE)/n(),
    zone_coverage_pct = sum(zone_coverage, na.rm = TRUE)/n(),
    .groups = "drop"
  )

# Save CSV
def_file <- paste0(OUTPUT_DIR, "defense_tendencies_", min(SEASONS), "_", max(SEASONS), ".csv")
write_csv(def_tendencies, def_file)
cat("Saved improved defensive tendencies to", def_file, "\n")

cat("Fetching play-by-play data for seasons:", SEASONS, "\n")
pbp <- load_pbp(SEASONS)


cat("Done!\n")
