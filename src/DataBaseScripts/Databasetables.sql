-- sabana_sin_rad definition

-- Drop table

-- DROP TABLE sabana_sin_rad;

CREATE TABLE paper.sabana_sin_rad (
	phenologystageid float8 NULL,
	variety text NULL,
	cadastralcode text NULL,
	longitude float8 NULL,
	latitude float8 NULL,
	altitudeASL float8 NULL,
	PDO_id text NULL,
	"date" timestamp NULL,
	station text NULL,
	season text NULL,
	tmed_min float8 NULL,
	"tmed_min 1_days_before" float8 NULL,
	"tmed_min 2_days_before" float8 NULL,
	"tmed_min 3_days_before" float8 NULL,
	"tmed_min 4_days_before" float8 NULL,
	"tmed_min 5_days_before" float8 NULL,
	"tmed_min 6_days_before" float8 NULL,
	"tmed_min 7_days_before" float8 NULL,
	"tmed_min 8_days_before" float8 NULL,
	"tmed_min 9_days_before" float8 NULL,
	"tmed_min 10_days_before" float8 NULL,
	"tmed_min 11_days_before" float8 NULL,
	"tmed_min 12_days_before" float8 NULL,
	"tmed_min 13_days_before" float8 NULL,
	"tmed_min 1_days_after" float8 NULL,
	"tmed_min 2_days_after" float8 NULL,
	"tmed_min 3_days_after" float8 NULL,
	"tmed_min 4_days_after" float8 NULL,
	"tmed_min 5_days_after" float8 NULL,
	"tmed_min 6_days_after" float8 NULL,
	rad_min float8 NULL,
	"rad_min 1_days_before" float8 NULL,
	"rad_min 2_days_before" float8 NULL,
	"rad_min 3_days_before" float8 NULL,
	"rad_min 4_days_before" float8 NULL,
	"rad_min 5_days_before" float8 NULL,
	"rad_min 6_days_before" float8 NULL,
	"rad_min 7_days_before" float8 NULL,
	"rad_min 8_days_before" float8 NULL,
	"rad_min 9_days_before" float8 NULL,
	"rad_min 10_days_before" float8 NULL,
	"rad_min 11_days_before" float8 NULL,
	"rad_min 12_days_before" float8 NULL,
	"rad_min 13_days_before" float8 NULL,
	"rad_min 1_days_after" float8 NULL,
	"rad_min 2_days_after" float8 NULL,
	"rad_min 3_days_after" float8 NULL,
	"rad_min 4_days_after" float8 NULL,
	"rad_min 5_days_after" float8 NULL,
	"rad_min 6_days_after" float8 NULL,
	"tmed_MAX" float8 NULL,
	"tmed_MAX 1_days_before" float8 NULL,
	"tmed_MAX 2_days_before" float8 NULL,
	"tmed_MAX 3_days_before" float8 NULL,
	"tmed_MAX 4_days_before" float8 NULL,
	"tmed_MAX 5_days_before" float8 NULL,
	"tmed_MAX 6_days_before" float8 NULL,
	"tmed_MAX 7_days_before" float8 NULL,
	"tmed_MAX 8_days_before" float8 NULL,
	"tmed_MAX 9_days_before" float8 NULL,
	"tmed_MAX 10_days_before" float8 NULL,
	"tmed_MAX 11_days_before" float8 NULL,
	"tmed_MAX 12_days_before" float8 NULL,
	"tmed_MAX 13_days_before" float8 NULL,
	"tmed_MAX 1_days_after" float8 NULL,
	"tmed_MAX 2_days_after" float8 NULL,
	"tmed_MAX 3_days_after" float8 NULL,
	"tmed_MAX 4_days_after" float8 NULL,
	"tmed_MAX 5_days_after" float8 NULL,
	"tmed_MAX 6_days_after" float8 NULL,
	"rad_MAX" float8 NULL,
	"rad_MAX 1_days_before" float8 NULL,
	"rad_MAX 2_days_before" float8 NULL,
	"rad_MAX 3_days_before" float8 NULL,
	"rad_MAX 4_days_before" float8 NULL,
	"rad_MAX 5_days_before" float8 NULL,
	"rad_MAX 6_days_before" float8 NULL,
	"rad_MAX 7_days_before" float8 NULL,
	"rad_MAX 8_days_before" float8 NULL,
	"rad_MAX 9_days_before" float8 NULL,
	"rad_MAX 10_days_before" float8 NULL,
	"rad_MAX 11_days_before" float8 NULL,
	"rad_MAX 12_days_before" float8 NULL,
	"rad_MAX 13_days_before" float8 NULL,
	"rad_MAX 1_days_after" float8 NULL,
	"rad_MAX 2_days_after" float8 NULL,
	"rad_MAX 3_days_after" float8 NULL,
	"rad_MAX 4_days_after" float8 NULL,
	"rad_MAX 5_days_after" float8 NULL,
	"rad_MAX 6_days_after" float8 NULL,
	tmed_mean float8 NULL,
	"tmed_mean 1_days_before" float8 NULL,
	"tmed_mean 2_days_before" float8 NULL,
	"tmed_mean 3_days_before" float8 NULL,
	"tmed_mean 4_days_before" float8 NULL,
	"tmed_mean 5_days_before" float8 NULL,
	"tmed_mean 6_days_before" float8 NULL,
	"tmed_mean 7_days_before" float8 NULL,
	"tmed_mean 8_days_before" float8 NULL,
	"tmed_mean 9_days_before" float8 NULL,
	"tmed_mean 10_days_before" float8 NULL,
	"tmed_mean 11_days_before" float8 NULL,
	"tmed_mean 12_days_before" float8 NULL,
	"tmed_mean 13_days_before" float8 NULL,
	"tmed_mean 1_days_after" float8 NULL,
	"tmed_mean 2_days_after" float8 NULL,
	"tmed_mean 3_days_after" float8 NULL,
	"tmed_mean 4_days_after" float8 NULL,
	"tmed_mean 5_days_after" float8 NULL,
	"tmed_mean 6_days_after" float8 NULL,
	rad_mean float8 NULL,
	"rad_mean 1_days_before" float8 NULL,
	"rad_mean 2_days_before" float8 NULL,
	"rad_mean 3_days_before" float8 NULL,
	"rad_mean 4_days_before" float8 NULL,
	"rad_mean 5_days_before" float8 NULL,
	"rad_mean 6_days_before" float8 NULL,
	"rad_mean 7_days_before" float8 NULL,
	"rad_mean 8_days_before" float8 NULL,
	"rad_mean 9_days_before" float8 NULL,
	"rad_mean 10_days_before" float8 NULL,
	"rad_mean 11_days_before" float8 NULL,
	"rad_mean 12_days_before" float8 NULL,
	"rad_mean 13_days_before" float8 NULL,
	"rad_mean 1_days_after" float8 NULL,
	"rad_mean 2_days_after" float8 NULL,
	"rad_mean 3_days_after" float8 NULL,
	"rad_mean 4_days_after" float8 NULL,
	"rad_mean 5_days_after" float8 NULL,
	"rad_mean 6_days_after" float8 NULL,
	hr_mean float8 NULL,
	"hr_mean 1_days_before" float8 NULL,
	"hr_mean 2_days_before" float8 NULL,
	"hr_mean 3_days_before" float8 NULL,
	"hr_mean 4_days_before" float8 NULL,
	"hr_mean 5_days_before" float8 NULL,
	"hr_mean 6_days_before" float8 NULL,
	"hr_mean 7_days_before" float8 NULL,
	"hr_mean 8_days_before" float8 NULL,
	"hr_mean 9_days_before" float8 NULL,
	"hr_mean 10_days_before" float8 NULL,
	"hr_mean 11_days_before" float8 NULL,
	"hr_mean 12_days_before" float8 NULL,
	"hr_mean 13_days_before" float8 NULL,
	"hr_mean 1_days_after" float8 NULL,
	"hr_mean 2_days_after" float8 NULL,
	"hr_mean 3_days_after" float8 NULL,
	"hr_mean 4_days_after" float8 NULL,
	"hr_mean 5_days_after" float8 NULL,
	"hr_mean 6_days_after" float8 NULL,
	"wind_N" float8 NULL,
	"wind_N 1_days_before" float8 NULL,
	"wind_N 2_days_before" float8 NULL,
	"wind_N 3_days_before" float8 NULL,
	"wind_N 4_days_before" float8 NULL,
	"wind_N 5_days_before" float8 NULL,
	"wind_N 6_days_before" float8 NULL,
	"wind_N 7_days_before" float8 NULL,
	"wind_N 8_days_before" float8 NULL,
	"wind_N 9_days_before" float8 NULL,
	"wind_N 10_days_before" float8 NULL,
	"wind_N 11_days_before" float8 NULL,
	"wind_N 12_days_before" float8 NULL,
	"wind_N 13_days_before" float8 NULL,
	"wind_N 1_days_after" float8 NULL,
	"wind_N 2_days_after" float8 NULL,
	"wind_N 3_days_after" float8 NULL,
	"wind_N 4_days_after" float8 NULL,
	"wind_N 5_days_after" float8 NULL,
	"wind_N 6_days_after" float8 NULL,
	"wind_NE" float8 NULL,
	"wind_NE 1_days_before" float8 NULL,
	"wind_NE 2_days_before" float8 NULL,
	"wind_NE 3_days_before" float8 NULL,
	"wind_NE 4_days_before" float8 NULL,
	"wind_NE 5_days_before" float8 NULL,
	"wind_NE 6_days_before" float8 NULL,
	"wind_NE 7_days_before" float8 NULL,
	"wind_NE 8_days_before" float8 NULL,
	"wind_NE 9_days_before" float8 NULL,
	"wind_NE 10_days_before" float8 NULL,
	"wind_NE 11_days_before" float8 NULL,
	"wind_NE 12_days_before" float8 NULL,
	"wind_NE 13_days_before" float8 NULL,
	"wind_NE 1_days_after" float8 NULL,
	"wind_NE 2_days_after" float8 NULL,
	"wind_NE 3_days_after" float8 NULL,
	"wind_NE 4_days_after" float8 NULL,
	"wind_NE 5_days_after" float8 NULL,
	"wind_NE 6_days_after" float8 NULL,
	"wind_E" float8 NULL,
	"wind_E 1_days_before" float8 NULL,
	"wind_E 2_days_before" float8 NULL,
	"wind_E 3_days_before" float8 NULL,
	"wind_E 4_days_before" float8 NULL,
	"wind_E 5_days_before" float8 NULL,
	"wind_E 6_days_before" float8 NULL,
	"wind_E 7_days_before" float8 NULL,
	"wind_E 8_days_before" float8 NULL,
	"wind_E 9_days_before" float8 NULL,
	"wind_E 10_days_before" float8 NULL,
	"wind_E 11_days_before" float8 NULL,
	"wind_E 12_days_before" float8 NULL,
	"wind_E 13_days_before" float8 NULL,
	"wind_E 1_days_after" float8 NULL,
	"wind_E 2_days_after" float8 NULL,
	"wind_E 3_days_after" float8 NULL,
	"wind_E 4_days_after" float8 NULL,
	"wind_E 5_days_after" float8 NULL,
	"wind_E 6_days_after" float8 NULL,
	"wind_SE" float8 NULL,
	"wind_SE 1_days_before" float8 NULL,
	"wind_SE 2_days_before" float8 NULL,
	"wind_SE 3_days_before" float8 NULL,
	"wind_SE 4_days_before" float8 NULL,
	"wind_SE 5_days_before" float8 NULL,
	"wind_SE 6_days_before" float8 NULL,
	"wind_SE 7_days_before" float8 NULL,
	"wind_SE 8_days_before" float8 NULL,
	"wind_SE 9_days_before" float8 NULL,
	"wind_SE 10_days_before" float8 NULL,
	"wind_SE 11_days_before" float8 NULL,
	"wind_SE 12_days_before" float8 NULL,
	"wind_SE 13_days_before" float8 NULL,
	"wind_SE 1_days_after" float8 NULL,
	"wind_SE 2_days_after" float8 NULL,
	"wind_SE 3_days_after" float8 NULL,
	"wind_SE 4_days_after" float8 NULL,
	"wind_SE 5_days_after" float8 NULL,
	"wind_SE 6_days_after" float8 NULL,
	"wind_S" float8 NULL,
	"wind_S 1_days_before" float8 NULL,
	"wind_S 2_days_before" float8 NULL,
	"wind_S 3_days_before" float8 NULL,
	"wind_S 4_days_before" float8 NULL,
	"wind_S 5_days_before" float8 NULL,
	"wind_S 6_days_before" float8 NULL,
	"wind_S 7_days_before" float8 NULL,
	"wind_S 8_days_before" float8 NULL,
	"wind_S 9_days_before" float8 NULL,
	"wind_S 10_days_before" float8 NULL,
	"wind_S 11_days_before" float8 NULL,
	"wind_S 12_days_before" float8 NULL,
	"wind_S 13_days_before" float8 NULL,
	"wind_S 1_days_after" float8 NULL,
	"wind_S 2_days_after" float8 NULL,
	"wind_S 3_days_after" float8 NULL,
	"wind_S 4_days_after" float8 NULL,
	"wind_S 5_days_after" float8 NULL,
	"wind_S 6_days_after" float8 NULL,
	"wind_SW" float8 NULL,
	"wind_SW 1_days_before" float8 NULL,
	"wind_SW 2_days_before" float8 NULL,
	"wind_SW 3_days_before" float8 NULL,
	"wind_SW 4_days_before" float8 NULL,
	"wind_SW 5_days_before" float8 NULL,
	"wind_SW 6_days_before" float8 NULL,
	"wind_SW 7_days_before" float8 NULL,
	"wind_SW 8_days_before" float8 NULL,
	"wind_SW 9_days_before" float8 NULL,
	"wind_SW 10_days_before" float8 NULL,
	"wind_SW 11_days_before" float8 NULL,
	"wind_SW 12_days_before" float8 NULL,
	"wind_SW 13_days_before" float8 NULL,
	"wind_SW 1_days_after" float8 NULL,
	"wind_SW 2_days_after" float8 NULL,
	"wind_SW 3_days_after" float8 NULL,
	"wind_SW 4_days_after" float8 NULL,
	"wind_SW 5_days_after" float8 NULL,
	"wind_SW 6_days_after" float8 NULL,
	"wind_W" float8 NULL,
	"wind_W 1_days_before" float8 NULL,
	"wind_W 2_days_before" float8 NULL,
	"wind_W 3_days_before" float8 NULL,
	"wind_W 4_days_before" float8 NULL,
	"wind_W 5_days_before" float8 NULL,
	"wind_W 6_days_before" float8 NULL,
	"wind_W 7_days_before" float8 NULL,
	"wind_W 8_days_before" float8 NULL,
	"wind_W 9_days_before" float8 NULL,
	"wind_W 10_days_before" float8 NULL,
	"wind_W 11_days_before" float8 NULL,
	"wind_W 12_days_before" float8 NULL,
	"wind_W 13_days_before" float8 NULL,
	"wind_W 1_days_after" float8 NULL,
	"wind_W 2_days_after" float8 NULL,
	"wind_W 3_days_after" float8 NULL,
	"wind_W 4_days_after" float8 NULL,
	"wind_W 5_days_after" float8 NULL,
	"wind_W 6_days_after" float8 NULL,
	"wind_NW" float8 NULL,
	"wind_NW 1_days_before" float8 NULL,
	"wind_NW 2_days_before" float8 NULL,
	"wind_NW 3_days_before" float8 NULL,
	"wind_NW 4_days_before" float8 NULL,
	"wind_NW 5_days_before" float8 NULL,
	"wind_NW 6_days_before" float8 NULL,
	"wind_NW 7_days_before" float8 NULL,
	"wind_NW 8_days_before" float8 NULL,
	"wind_NW 9_days_before" float8 NULL,
	"wind_NW 10_days_before" float8 NULL,
	"wind_NW 11_days_before" float8 NULL,
	"wind_NW 12_days_before" float8 NULL,
	"wind_NW 13_days_before" float8 NULL,
	"wind_NW 1_days_after" float8 NULL,
	"wind_NW 2_days_after" float8 NULL,
	"wind_NW 3_days_after" float8 NULL,
	"wind_NW 4_days_after" float8 NULL,
	"wind_NW 5_days_after" float8 NULL,
	"wind_NW 6_days_after" float8 NULL,
	"gdd_4.5_t0_Tbase_sum" float8 NULL,
	"gdd_4.5_t0_Tbase_sum 1_weeks_before" float8 NULL,
	"gdd_4.5_t0_Tbase_sum 2_weeks_before" float8 NULL,
	"gdd_4.5_t0_Tbase_sum 1_weeks_after" float8 NULL,
	"gdd_4.5_t0_TbaseMAX_sum" float8 NULL,
	"gdd_4.5_t0_TbaseMAX_sum 1_weeks_before" float8 NULL,
	"gdd_4.5_t0_TbaseMAX_sum 2_weeks_before" float8 NULL,
	"gdd_4.5_t0_TbaseMAX_sum 1_weeks_after" float8 NULL,
	"gdd_4.5_1_Tbase_sum" float8 NULL,
	"gdd_4.5_1_Tbase_sum 1_weeks_before" float8 NULL,
	"gdd_4.5_1_Tbase_sum 2_weeks_before" float8 NULL,
	"gdd_4.5_1_Tbase_sum 1_weeks_after" float8 NULL,
	"gdd_4.5_1_TbaseMAX_sum" float8 NULL,
	"gdd_4.5_1_TbaseMAX_sum 1_weeks_before" float8 NULL,
	"gdd_4.5_1_TbaseMAX_sum 2_weeks_before" float8 NULL,
	"gdd_4.5_1_TbaseMAX_sum 1_weeks_after" float8 NULL,
	"gdd_4.5_2_Tbase_sum" float8 NULL,
	"gdd_4.5_2_Tbase_sum 1_weeks_before" float8 NULL,
	"gdd_4.5_2_Tbase_sum 2_weeks_before" float8 NULL,
	"gdd_4.5_2_Tbase_sum 1_weeks_after" float8 NULL,
	"gdd_4.5_2_TbaseMAX_sum" float8 NULL,
	"gdd_4.5_2_TbaseMAX_sum 1_weeks_before" float8 NULL,
	"gdd_4.5_2_TbaseMAX_sum 2_weeks_before" float8 NULL,
	"gdd_4.5_2_TbaseMAX_sum 1_weeks_after" float8 NULL,
	"gdd_10.0_t0_Tbase_sum" float8 NULL,
	"gdd_10.0_t0_Tbase_sum 1_weeks_before" float8 NULL,
	"gdd_10.0_t0_Tbase_sum 2_weeks_before" float8 NULL,
	"gdd_10.0_t0_Tbase_sum 1_weeks_after" float8 NULL,
	"gdd_10.0_t0_TbaseMAX_sum" float8 NULL,
	"gdd_10.0_t0_TbaseMAX_sum 1_weeks_before" float8 NULL,
	"gdd_10.0_t0_TbaseMAX_sum 2_weeks_before" float8 NULL,
	"gdd_10.0_t0_TbaseMAX_sum 1_weeks_after" float8 NULL,
	"gdd_10.0_1_Tbase_sum" float8 NULL,
	"gdd_10.0_1_Tbase_sum 1_weeks_before" float8 NULL,
	"gdd_10.0_1_Tbase_sum 2_weeks_before" float8 NULL,
	"gdd_10.0_1_Tbase_sum 1_weeks_after" float8 NULL,
	"gdd_10.0_1_TbaseMAX_sum" float8 NULL,
	"gdd_10.0_1_TbaseMAX_sum 1_weeks_before" float8 NULL,
	"gdd_10.0_1_TbaseMAX_sum 2_weeks_before" float8 NULL,
	"gdd_10.0_1_TbaseMAX_sum 1_weeks_after" float8 NULL,
	"gdd_10.0_2_Tbase_sum" float8 NULL,
	"gdd_10.0_2_Tbase_sum 1_weeks_before" float8 NULL,
	"gdd_10.0_2_Tbase_sum 2_weeks_before" float8 NULL,
	"gdd_10.0_2_Tbase_sum 1_weeks_after" float8 NULL,
	"gdd_10.0_2_TbaseMAX_sum" float8 NULL,
	"gdd_10.0_2_TbaseMAX_sum 1_weeks_before" float8 NULL,
	"gdd_10.0_2_TbaseMAX_sum 2_weeks_before" float8 NULL,
	"gdd_10.0_2_TbaseMAX_sum 1_weeks_after" float8 NULL,
	"chillingDD_7.0_t0_Tbase_sum" float8 NULL,
	"chillingDD_7.0_t0_Tbase_sum 1_weeks_before" float8 NULL,
	"chillingDD_7.0_t0_Tbase_sum 2_weeks_before" float8 NULL,
	"chillingDD_7.0_t0_Tbase_sum 1_weeks_after" float8 NULL,
	"chillingDD_7.0_t0_Tbasemin_sum" float8 NULL,
	"chillingDD_7.0_t0_Tbasemin_sum 1_weeks_before" float8 NULL,
	"chillingDD_7.0_t0_Tbasemin_sum 2_weeks_before" float8 NULL,
	"chillingDD_7.0_t0_Tbasemin_sum 1_weeks_after" float8 NULL,
	"chillingDD_7.0_t0_Utah_sum" float8 NULL,
	"chillingDD_7.0_t0_Utah_sum 1_weeks_before" float8 NULL,
	"chillingDD_7.0_t0_Utah_sum 2_weeks_before" float8 NULL,
	"chillingDD_7.0_t0_Utah_sum 1_weeks_after" float8 NULL,
	"chillingDD_7.0_1_Tbase_sum" float8 NULL,
	"chillingDD_7.0_1_Tbase_sum 1_weeks_before" float8 NULL,
	"chillingDD_7.0_1_Tbase_sum 2_weeks_before" float8 NULL,
	"chillingDD_7.0_1_Tbase_sum 1_weeks_after" float8 NULL,
	"chillingDD_7.0_1_Tbasemin_sum" float8 NULL,
	"chillingDD_7.0_1_Tbasemin_sum 1_weeks_before" float8 NULL,
	"chillingDD_7.0_1_Tbasemin_sum 2_weeks_before" float8 NULL,
	"chillingDD_7.0_1_Tbasemin_sum 1_weeks_after" float8 NULL,
	"chillingDD_7.0_1_Utah_sum" float8 NULL,
	"chillingDD_7.0_1_Utah_sum 1_weeks_before" float8 NULL,
	"chillingDD_7.0_1_Utah_sum 2_weeks_before" float8 NULL,
	"chillingDD_7.0_1_Utah_sum 1_weeks_after" float8 NULL,
	"chillingDD_7.0_2_Tbase_sum" float8 NULL,
	"chillingDD_7.0_2_Tbase_sum 1_weeks_before" float8 NULL,
	"chillingDD_7.0_2_Tbase_sum 2_weeks_before" float8 NULL,
	"chillingDD_7.0_2_Tbase_sum 1_weeks_after" float8 NULL,
	"chillingDD_7.0_2_Tbasemin_sum" float8 NULL,
	"chillingDD_7.0_2_Tbasemin_sum 1_weeks_before" float8 NULL,
	"chillingDD_7.0_2_Tbasemin_sum 2_weeks_before" float8 NULL,
	"chillingDD_7.0_2_Tbasemin_sum 1_weeks_after" float8 NULL,
	"chillingDD_7.0_2_Utah_sum" float8 NULL,
	"chillingDD_7.0_2_Utah_sum 1_weeks_before" float8 NULL,
	"chillingDD_7.0_2_Utah_sum 2_weeks_before" float8 NULL,
	"chillingDD_7.0_2_Utah_sum 1_weeks_after" float8 NULL,
	rad_sum float8 NULL,
	"rad_sum 1_weeks_before" float8 NULL,
	"rad_sum 2_weeks_before" float8 NULL,
	"rad_sum 1_weeks_after" float8 NULL,
	precip_sum float8 NULL,
	"precip_sum 1_weeks_before" float8 NULL,
	"precip_sum 2_weeks_before" float8 NULL,
	"precip_sum 1_weeks_after" float8 NULL,
	"winkler_4.5_Tbase" float8 NULL,
	"winkler_4.5_Tbase 1_weeks_before" float8 NULL,
	"winkler_4.5_Tbase 2_weeks_before" float8 NULL,
	"winkler_4.5_Tbase 1_weeks_after" float8 NULL,
	"winkler_4.5_TbaseMAX" float8 NULL,
	"winkler_4.5_TbaseMAX 1_weeks_before" float8 NULL,
	"winkler_4.5_TbaseMAX 2_weeks_before" float8 NULL,
	"winkler_4.5_TbaseMAX 1_weeks_after" float8 NULL,
	"winkler_10.0_Tbase" float8 NULL,
	"winkler_10.0_Tbase 1_weeks_before" float8 NULL,
	"winkler_10.0_Tbase 2_weeks_before" float8 NULL,
	"winkler_10.0_Tbase 1_weeks_after" float8 NULL,
	"winkler_10.0_TbaseMAX" float8 NULL,
	"winkler_10.0_TbaseMAX 1_weeks_before" float8 NULL,
	"winkler_10.0_TbaseMAX 2_weeks_before" float8 NULL,
	"winkler_10.0_TbaseMAX 1_weeks_after" float8 NULL,
	"gdd_4.5_t0_Tbase_sum_Cumm" float8 NULL,
	"gdd_4.5_t0_Tbase_sum_Cumm 1_weeks_before" float8 NULL,
	"gdd_4.5_t0_Tbase_sum_Cumm 2_weeks_before" float8 NULL,
	"gdd_4.5_t0_Tbase_sum_Cumm 1_weeks_after" float8 NULL,
	"gdd_4.5_t0_TbaseMAX_sum_Cumm" float8 NULL,
	"gdd_4.5_t0_TbaseMAX_sum_Cumm 1_weeks_before" float8 NULL,
	"gdd_4.5_t0_TbaseMAX_sum_Cumm 2_weeks_before" float8 NULL,
	"gdd_4.5_t0_TbaseMAX_sum_Cumm 1_weeks_after" float8 NULL,
	"gdd_4.5_1_Tbase_sum_Cumm" float8 NULL,
	"gdd_4.5_1_Tbase_sum_Cumm 1_weeks_before" float8 NULL,
	"gdd_4.5_1_Tbase_sum_Cumm 2_weeks_before" float8 NULL,
	"gdd_4.5_1_Tbase_sum_Cumm 1_weeks_after" float8 NULL,
	"gdd_4.5_1_TbaseMAX_sum_Cumm" float8 NULL,
	"gdd_4.5_1_TbaseMAX_sum_Cumm 1_weeks_before" float8 NULL,
	"gdd_4.5_1_TbaseMAX_sum_Cumm 2_weeks_before" float8 NULL,
	"gdd_4.5_1_TbaseMAX_sum_Cumm 1_weeks_after" float8 NULL,
	"gdd_4.5_2_Tbase_sum_Cumm" float8 NULL,
	"gdd_4.5_2_Tbase_sum_Cumm 1_weeks_before" float8 NULL,
	"gdd_4.5_2_Tbase_sum_Cumm 2_weeks_before" float8 NULL,
	"gdd_4.5_2_Tbase_sum_Cumm 1_weeks_after" float8 NULL,
	"gdd_4.5_2_TbaseMAX_sum_Cumm" float8 NULL,
	"gdd_4.5_2_TbaseMAX_sum_Cumm 1_weeks_before" float8 NULL,
	"gdd_4.5_2_TbaseMAX_sum_Cumm 2_weeks_before" float8 NULL,
	"gdd_4.5_2_TbaseMAX_sum_Cumm 1_weeks_after" float8 NULL,
	"gdd_10.0_t0_Tbase_sum_Cumm" float8 NULL,
	"gdd_10.0_t0_Tbase_sum_Cumm 1_weeks_before" float8 NULL,
	"gdd_10.0_t0_Tbase_sum_Cumm 2_weeks_before" float8 NULL,
	"gdd_10.0_t0_Tbase_sum_Cumm 1_weeks_after" float8 NULL,
	"gdd_10.0_t0_TbaseMAX_sum_Cumm" float8 NULL,
	"gdd_10.0_t0_TbaseMAX_sum_Cumm 1_weeks_before" float8 NULL,
	"gdd_10.0_t0_TbaseMAX_sum_Cumm 2_weeks_before" float8 NULL,
	"gdd_10.0_t0_TbaseMAX_sum_Cumm 1_weeks_after" float8 NULL,
	"gdd_10.0_1_Tbase_sum_Cumm" float8 NULL,
	"gdd_10.0_1_Tbase_sum_Cumm 1_weeks_before" float8 NULL,
	"gdd_10.0_1_Tbase_sum_Cumm 2_weeks_before" float8 NULL,
	"gdd_10.0_1_Tbase_sum_Cumm 1_weeks_after" float8 NULL,
	"gdd_10.0_1_TbaseMAX_sum_Cumm" float8 NULL,
	"gdd_10.0_1_TbaseMAX_sum_Cumm 1_weeks_before" float8 NULL,
	"gdd_10.0_1_TbaseMAX_sum_Cumm 2_weeks_before" float8 NULL,
	"gdd_10.0_1_TbaseMAX_sum_Cumm 1_weeks_after" float8 NULL,
	"gdd_10.0_2_Tbase_sum_Cumm" float8 NULL,
	"gdd_10.0_2_Tbase_sum_Cumm 1_weeks_before" float8 NULL,
	"gdd_10.0_2_Tbase_sum_Cumm 2_weeks_before" float8 NULL,
	"gdd_10.0_2_Tbase_sum_Cumm 1_weeks_after" float8 NULL,
	"gdd_10.0_2_TbaseMAX_sum_Cumm" float8 NULL,
	"gdd_10.0_2_TbaseMAX_sum_Cumm 1_weeks_before" float8 NULL,
	"gdd_10.0_2_TbaseMAX_sum_Cumm 2_weeks_before" float8 NULL,
	"gdd_10.0_2_TbaseMAX_sum_Cumm 1_weeks_after" float8 NULL,
	"chillingDD_7.0_t0_Tbase_sum_Cumm" float8 NULL,
	"chillingDD_7.0_t0_Tbase_sum_Cumm 1_weeks_before" float8 NULL,
	"chillingDD_7.0_t0_Tbase_sum_Cumm 2_weeks_before" float8 NULL,
	"chillingDD_7.0_t0_Tbase_sum_Cumm 1_weeks_after" float8 NULL,
	"chillingDD_7.0_t0_Tbasemin_sum_Cumm" float8 NULL,
	"chillingDD_7.0_t0_Tbasemin_sum_Cumm 1_weeks_before" float8 NULL,
	"chillingDD_7.0_t0_Tbasemin_sum_Cumm 2_weeks_before" float8 NULL,
	"chillingDD_7.0_t0_Tbasemin_sum_Cumm 1_weeks_after" float8 NULL,
	"chillingDD_7.0_t0_Utah_sum_Cumm" float8 NULL,
	"chillingDD_7.0_t0_Utah_sum_Cumm 1_weeks_before" float8 NULL,
	"chillingDD_7.0_t0_Utah_sum_Cumm 2_weeks_before" float8 NULL,
	"chillingDD_7.0_t0_Utah_sum_Cumm 1_weeks_after" float8 NULL,
	"chillingDD_7.0_1_Tbase_sum_Cumm" float8 NULL,
	"chillingDD_7.0_1_Tbase_sum_Cumm 1_weeks_before" float8 NULL,
	"chillingDD_7.0_1_Tbase_sum_Cumm 2_weeks_before" float8 NULL,
	"chillingDD_7.0_1_Tbase_sum_Cumm 1_weeks_after" float8 NULL,
	"chillingDD_7.0_1_Tbasemin_sum_Cumm" float8 NULL,
	"chillingDD_7.0_1_Tbasemin_sum_Cumm 1_weeks_before" float8 NULL,
	"chillingDD_7.0_1_Tbasemin_sum_Cumm 2_weeks_before" float8 NULL,
	"chillingDD_7.0_1_Tbasemin_sum_Cumm 1_weeks_after" float8 NULL,
	"chillingDD_7.0_1_Utah_sum_Cumm" float8 NULL,
	"chillingDD_7.0_1_Utah_sum_Cumm 1_weeks_before" float8 NULL,
	"chillingDD_7.0_1_Utah_sum_Cumm 2_weeks_before" float8 NULL,
	"chillingDD_7.0_1_Utah_sum_Cumm 1_weeks_after" float8 NULL,
	"chillingDD_7.0_2_Tbase_sum_Cumm" float8 NULL,
	"chillingDD_7.0_2_Tbase_sum_Cumm 1_weeks_before" float8 NULL,
	"chillingDD_7.0_2_Tbase_sum_Cumm 2_weeks_before" float8 NULL,
	"chillingDD_7.0_2_Tbase_sum_Cumm 1_weeks_after" float8 NULL,
	"chillingDD_7.0_2_Tbasemin_sum_Cumm" float8 NULL,
	"chillingDD_7.0_2_Tbasemin_sum_Cumm 1_weeks_before" float8 NULL,
	"chillingDD_7.0_2_Tbasemin_sum_Cumm 2_weeks_before" float8 NULL,
	"chillingDD_7.0_2_Tbasemin_sum_Cumm 1_weeks_after" float8 NULL,
	"chillingDD_7.0_2_Utah_sum_Cumm" float8 NULL,
	"chillingDD_7.0_2_Utah_sum_Cumm 1_weeks_before" float8 NULL,
	"chillingDD_7.0_2_Utah_sum_Cumm 2_weeks_before" float8 NULL,
	"chillingDD_7.0_2_Utah_sum_Cumm 1_weeks_after" float8 NULL,
	"rad__t0__Cumm" float8 NULL,
	"rad__t0__Cumm 1_weeks_before" float8 NULL,
	"rad__t0__Cumm 2_weeks_before" float8 NULL,
	"rad__t0__Cumm 1_weeks_after" float8 NULL,
	"rad__1__Cumm" float8 NULL,
	"rad__1__Cumm 1_weeks_before" float8 NULL,
	"rad__1__Cumm 2_weeks_before" float8 NULL,
	"rad__1__Cumm 1_weeks_after" float8 NULL,
	"rad__2__Cumm" float8 NULL,
	"rad__2__Cumm 1_weeks_before" float8 NULL,
	"rad__2__Cumm 2_weeks_before" float8 NULL,
	"rad__2__Cumm 1_weeks_after" float8 NULL,
	"precip__t0__Cumm" float8 NULL,
	"precip__t0__Cumm 1_weeks_before" float8 NULL,
	"precip__t0__Cumm 2_weeks_before" float8 NULL,
	"precip__t0__Cumm 1_weeks_after" float8 NULL,
	"precip__1__Cumm" float8 NULL,
	"precip__1__Cumm 1_weeks_before" float8 NULL,
	"precip__1__Cumm 2_weeks_before" float8 NULL,
	"precip__1__Cumm 1_weeks_after" float8 NULL,
	"precip__2__Cumm" float8 NULL,
	"precip__2__Cumm 1_weeks_before" float8 NULL,
	"precip__2__Cumm 2_weeks_before" float8 NULL,
	"precip__2__Cumm 1_weeks_after" float8 NULL,
	"winkler_4.5_t0_Tbase_Cumm" float8 NULL,
	"winkler_4.5_t0_Tbase_Cumm 1_weeks_before" float8 NULL,
	"winkler_4.5_t0_Tbase_Cumm 2_weeks_before" float8 NULL,
	"winkler_4.5_t0_Tbase_Cumm 1_weeks_after" float8 NULL,
	"winkler_4.5_t0_TbaseMAX_Cumm" float8 NULL,
	"winkler_4.5_t0_TbaseMAX_Cumm 1_weeks_before" float8 NULL,
	"winkler_4.5_t0_TbaseMAX_Cumm 2_weeks_before" float8 NULL,
	"winkler_4.5_t0_TbaseMAX_Cumm 1_weeks_after" float8 NULL,
	"winkler_4.5_1_Tbase_Cumm" float8 NULL,
	"winkler_4.5_1_Tbase_Cumm 1_weeks_before" float8 NULL,
	"winkler_4.5_1_Tbase_Cumm 2_weeks_before" float8 NULL,
	"winkler_4.5_1_Tbase_Cumm 1_weeks_after" float8 NULL,
	"winkler_4.5_1_TbaseMAX_Cumm" float8 NULL,
	"winkler_4.5_1_TbaseMAX_Cumm 1_weeks_before" float8 NULL,
	"winkler_4.5_1_TbaseMAX_Cumm 2_weeks_before" float8 NULL,
	"winkler_4.5_1_TbaseMAX_Cumm 1_weeks_after" float8 NULL,
	"winkler_4.5_2_Tbase_Cumm" float8 NULL,
	"winkler_4.5_2_Tbase_Cumm 1_weeks_before" float8 NULL,
	"winkler_4.5_2_Tbase_Cumm 2_weeks_before" float8 NULL,
	"winkler_4.5_2_Tbase_Cumm 1_weeks_after" float8 NULL,
	"winkler_4.5_2_TbaseMAX_Cumm" float8 NULL,
	"winkler_4.5_2_TbaseMAX_Cumm 1_weeks_before" float8 NULL,
	"winkler_4.5_2_TbaseMAX_Cumm 2_weeks_before" float8 NULL,
	"winkler_4.5_2_TbaseMAX_Cumm 1_weeks_after" float8 NULL,
	"winkler_10.0_t0_Tbase_Cumm" float8 NULL,
	"winkler_10.0_t0_Tbase_Cumm 1_weeks_before" float8 NULL,
	"winkler_10.0_t0_Tbase_Cumm 2_weeks_before" float8 NULL,
	"winkler_10.0_t0_Tbase_Cumm 1_weeks_after" float8 NULL,
	"winkler_10.0_t0_TbaseMAX_Cumm" float8 NULL,
	"winkler_10.0_t0_TbaseMAX_Cumm 1_weeks_before" float8 NULL,
	"winkler_10.0_t0_TbaseMAX_Cumm 2_weeks_before" float8 NULL,
	"winkler_10.0_t0_TbaseMAX_Cumm 1_weeks_after" float8 NULL,
	"winkler_10.0_1_Tbase_Cumm" float8 NULL,
	"winkler_10.0_1_Tbase_Cumm 1_weeks_before" float8 NULL,
	"winkler_10.0_1_Tbase_Cumm 2_weeks_before" float8 NULL,
	"winkler_10.0_1_Tbase_Cumm 1_weeks_after" float8 NULL,
	"winkler_10.0_1_TbaseMAX_Cumm" float8 NULL,
	"winkler_10.0_1_TbaseMAX_Cumm 1_weeks_before" float8 NULL,
	"winkler_10.0_1_TbaseMAX_Cumm 2_weeks_before" float8 NULL,
	"winkler_10.0_1_TbaseMAX_Cumm 1_weeks_after" float8 NULL,
	"winkler_10.0_2_Tbase_Cumm" float8 NULL,
	"winkler_10.0_2_Tbase_Cumm 1_weeks_before" float8 NULL,
	"winkler_10.0_2_Tbase_Cumm 2_weeks_before" float8 NULL,
	"winkler_10.0_2_Tbase_Cumm 1_weeks_after" float8 NULL,
	"winkler_10.0_2_TbaseMAX_Cumm" float8 NULL,
	"winkler_10.0_2_TbaseMAX_Cumm 1_weeks_before" float8 NULL,
	"winkler_10.0_2_TbaseMAX_Cumm 2_weeks_before" float8 NULL,
	"winkler_10.0_2_TbaseMAX_Cumm 1_weeks_after" float8 NULL,
	min float8 NULL,
	MAX float8 NULL,
	mean float8 NULL,
	std float8 NULL,
	median float8 NULL,
	diff float8 NULL,
	"day" int8 NULL,
	"PDO_Borja" int8 NULL,
	"PDO_Calatayud" int8 NULL,
	"PDO_Carinena" int8 NULL,
	"PDO_Somontano" int8 NULL,
	"variety_CABERNET SAUVIGNON" int8 NULL,
	"variety_CHARDONNAY" int8 NULL,
	"variety_GARNACHA" int8 NULL,
	"variety_MAZUELA" int8 NULL,
	"variety_SYRACH" int8 NULL,
	"variety_TEMPRANILLO" int8 NULL
);
CREATE INDEX sabana_sin_rad_fecha_idx_2 ON paper.sabana_sin_rad USING btree ("date");


-- Permissions

ALTER TABLE paper.sabana_sin_rad OWNER TO postgres;
GRANT DELETE, TRIGGER, TRUNCATE, UPDATE, SELECT, REFERENCES, INSERT ON TABLE paper.sabana_sin_rad TO postgres;


-- paper.datoshorarios definition

-- Drop table

-- DROP TABLE paper.datoshorarios;

CREATE TABLE paper.datoshorarios (
	idprovincia int2 NOT NULL,
	idestacion varchar(10) NOT NULL,
	año int2 NOT NULL,
	dia int2 NOT NULL,
	fecha date NULL,
	horamin int2 NOT NULL,
	añosolar int2 NULL,
	diasolar int2 NULL,
	horaminsolar int2 NULL,
	tempmedia numeric(18, 2) NULL,
	codtempmedia int2 NULL,
	humedadmedia numeric(18, 2) NULL,
	codhumedadmedia int2 NULL,
	velviento numeric(18, 2) NULL,
	codvelviento int2 NULL,
	dirviento numeric(18, 2) NULL,
	coddirviento int2 NULL,
	radiacion numeric(18, 2) NULL,
	codradiacion int2 NULL,
	precipitacion numeric(18, 2) NULL,
	codprecipitacion int2 NULL,
	tempmediacaja numeric(18, 2) NULL,
	codtempmediacaja int2 NULL,
	fechaultmod date NULL,
	tempsuelo1 numeric(18, 2) NULL,
	codtempsuelo1 int2 NULL,
	tempsuelo2 numeric(18, 2) NULL,
	codtempsuelo2 int2 NULL,
	CONSTRAINT restr_datoshorarios PRIMARY KEY (idprovincia, idestacion, "año", dia, horamin)
);

-- Permissions

ALTER TABLE paper.datoshorarios OWNER TO postgres;
GRANT INSERT, DELETE, UPDATE, REFERENCES, TRIGGER, TRUNCATE, SELECT ON TABLE paper.datoshorarios TO postgres;


-- paper.arahorario definition

-- Drop table

-- DROP TABLE paper.arahorario;

CREATE TABLE paper.arahorario (
	indicativo varchar(10) NOT NULL,
	ubi varchar(50) NULL,
	año int4 NULL,
	dia int4 NULL,
	fecha date NOT NULL,
	horamin int4 NOT NULL,
	tmed numeric(6, 2) NULL,
	hr numeric(6, 2) NULL,
	precip numeric(6, 2) NULL,
	vv numeric(6, 2) NULL,
	dv int4 NULL,
	humhoja numeric(6, 2) NULL,
	presion numeric(6, 2) NULL,
	rad numeric(6, 2) NULL,
	h30 numeric(6, 2) NULL,
	t30 numeric(6, 2) NULL,
	inso numeric(6, 2) NULL,
	bat int4 NULL,
	hora time NULL,
	"time" timestamp NULL,
	precipacumulada numeric(6, 2) NULL,
	"source" varchar(10) NULL,
	CONSTRAINT constr PRIMARY KEY (indicativo, fecha, horamin)
)
PARTITION BY RANGE (indicativo);
CREATE INDEX arahorario_fecha_idx_2 ON ONLY paper.arahorario USING btree (fecha);
CREATE INDEX arahorario_indicativo_idx_2 ON ONLY paper.arahorario USING btree (indicativo, fecha);

-- Permissions

ALTER TABLE paper.arahorario OWNER TO postgres;
GRANT INSERT, DELETE, UPDATE, REFERENCES, TRIGGER, TRUNCATE, SELECT ON TABLE paper.arahorario TO postgres;



-- paper.copernicus_nvdi_seq definition

-- DROP SEQUENCE paper.copernicus_nvdi_seq;

CREATE SEQUENCE paper.copernicus_nvdi_seq
	INCREMENT BY 1
	MINVALUE 1
	MAXVALUE 9223372036854775807
	START 1
	CACHE 1
	NO CYCLE;

-- Permissions

ALTER SEQUENCE paper.copernicus_nvdi_seq OWNER TO postgres;
GRANT ALL ON SEQUENCE paper.copernicus_nvdi_seq TO postgres;


-- paper.copernicus_nvdi definition

-- Drop table

-- DROP TABLE paper.copernicus_nvdi;

CREATE TABLE paper.copernicus_nvdi (
	id int8 DEFAULT nextval('paper.copernicus_nvdi_seq'::regclass) NOT NULL,
	codigo varchar(256) NULL,
	"date" date DEFAULT CURRENT_DATE NULL,
	min float8 NULL,
	max float8 NULL,
	mean float8 NULL,
	std float8 NULL,
	meidan float8 NULL,
	pixels_array text NULL,
	tesela varchar(256) NULL,
	CONSTRAINT copernicus_nvdi_pk PRIMARY KEY (id)
);

-- Permissions

ALTER TABLE paper.copernicus_nvdi OWNER TO postgres;
GRANT INSERT, DELETE, UPDATE, REFERENCES, TRIGGER, TRUNCATE, SELECT ON TABLE paper.copernicus_nvdi TO postgres;


CREATE OR REPLACE VIEW paper.datosestacionporyearactual
AS SELECT a.indicativo,
    a."año",
    count(a.fecha) AS days,
    sum(a.samplesperday) AS totalsamplesperday
   FROM ( SELECT DISTINCT arahorario.indicativo,
            arahorario."año",
            arahorario.fecha,
            count(*) AS samplesperday
           FROM arahorario
          WHERE arahorario."año"::double precision = date_part('year'::text, now())
          GROUP BY arahorario.indicativo, arahorario."año", arahorario.fecha) a
  GROUP BY a.indicativo, a."año"
  ORDER BY a.indicativo, a."año";

-- Permissions

ALTER TABLE paper.datosestacionporyearactual OWNER TO postgres;
GRANT ALL ON TABLE paper.datosestacionporyearactual TO postgres;


-- paper.datosestacionporyearantiguos source

CREATE OR REPLACE VIEW paper.datosestacionporyearantiguos
AS SELECT a.indicativo,
    a."año",
    count(a.fecha) AS days,
    sum(a.samplesperday) AS totalsamplesperday
   FROM ( SELECT DISTINCT arahorario.indicativo,
            arahorario."año",
            arahorario.fecha,
            count(*) AS samplesperday
           FROM arahorario
          WHERE arahorario."año"::double precision <= (date_part('year'::text, now()) - 1::double precision)
          GROUP BY arahorario.indicativo, arahorario."año", arahorario.fecha) a
  GROUP BY a.indicativo, a."año"
  ORDER BY a.indicativo, a."año";

-- Permissions

ALTER TABLE paper.datosestacionporyearantiguos OWNER TO postgres;
GRANT ALL ON TABLE paper.datosestacionporyearantiguos TO postgres;


-- paper.estaciones definition

-- Drop table

-- DROP TABLE paper.estaciones;

CREATE TABLE paper.estaciones (
	idprovincia int2 NOT NULL,
	idestacion varchar(10) NOT NULL,
	nombre varchar(50) NULL,
	longitud varchar(20) NULL,
	latitud varchar(20) NULL,
	altitud int2 NULL,
	xutm numeric(18, 1) NULL,
	yutm numeric(18, 1) NULL,
	huso int2 NULL,
	fechainstalacion timestamptz NULL,
	idtermino int2 NULL,
	nombrecorto varchar(6) NULL,
	metodocalculope varchar(20) NULL,
	estado varchar(20) NULL,
	CONSTRAINT restr_estaciones PRIMARY KEY (idprovincia, idestacion)
);

-- Permissions

ALTER TABLE paper.estaciones OWNER TO postgres;
GRANT INSERT, DELETE, UPDATE, REFERENCES, TRIGGER, TRUNCATE, SELECT ON TABLE paper.estaciones TO postgres;


-- paper.estaragon definition

-- Drop table

-- DROP TABLE paper.estaragon;

CREATE TABLE paper.estaragon (
	id int4 NOT NULL,
	geom geometry(pointz, 25830) NULL,
	indicativo varchar(254) NULL,
	nombre varchar(254) NULL,
	provincia varchar(254) NULL,
	altitud float8 NULL,
	datum varchar(254) NULL,
	idestacion varchar(10) NULL,
	ubi varchar(30) NULL,
	idprovincia int4 NULL,
	huso int8 NULL,
	idtermino int8 NULL,
	nombrecorto varchar(6) NULL,
	estado varchar(254) NULL,
	layer varchar(254) NULL,
	utmx int8 NULL,
	utmy int8 NULL,
	alt int8 NULL,
	long numeric(10, 6) NULL,
	lat numeric(10, 6) NULL,
	considered bool DEFAULT false NULL,
	"location" geometry NULL,
	pocid int8 NULL,
	nominalsamplesnumber int8 DEFAULT 48 NOT NULL,
	CONSTRAINT estaragon_pkey PRIMARY KEY (id)
);
CREATE INDEX estaragon_layer_idx_2 ON paper.estaragon USING btree (layer);
CREATE INDEX sidx_estaragon_geom_2 ON paper.estaragon USING gist (geom);

-- Permissions

ALTER TABLE paper.estaragon OWNER TO postgres;
GRANT INSERT, DELETE, UPDATE, REFERENCES, TRIGGER, TRUNCATE, SELECT ON TABLE paper.estaragon TO postgres;



-- paper.gv_estaragonrestringido source

CREATE OR REPLACE VIEW paper.gv_estaragonrestringido
AS SELECT estaragon.id,
    estaragon.geom,
    estaragon.indicativo,
    estaragon.nombre,
    estaragon.provincia,
    estaragon.altitud,
    estaragon.datum,
    estaragon.idestacion,
    estaragon.ubi,
    estaragon.idprovincia,
    estaragon.huso,
    estaragon.idtermino,
    estaragon.nombrecorto,
    estaragon.estado,
    estaragon.layer,
    estaragon.utmx,
    estaragon.utmy,
    estaragon.alt,
    estaragon.long,
    estaragon.lat,
    estaragon.considered,
    estaragon.location,
    estaragon.pocid,
    estaragon.nominalsamplesnumber
   FROM paper.estaragon
  WHERE NOT estaragon.indicativo::text ~ '^[0-9].*'::text AND estaragon.indicativo::text <> 'Z28'::text AND estaragon.idestacion::text <> 'gv13baja'::text AND estaragon.considered;

-- Permissions

ALTER TABLE paper.gv_estaragonrestringido OWNER TO postgres;
GRANT ALL ON TABLE paper.gv_estaragonrestringido TO postgres;



-- paper.gv_estaragon24restringido source

CREATE OR REPLACE VIEW paper.gv_estaragon24restringido
AS SELECT ea.id,
    ea.geom,
    ea.indicativo,
    ea.nombre,
    ea.provincia,
    ea.altitud,
    ea.datum,
    ea.idestacion,
    ea.ubi,
    ea.idprovincia,
    ea.huso,
    ea.idtermino,
    ea.nombrecorto,
    ea.estado,
    ea.layer,
    ea.utmx,
    ea.utmy,
    ea.alt,
    ea.long,
    ea.lat,
    ea.considered,
    ea.location,
    ea.pocid,
    ea.nominalsamplesnumber
   FROM paper.gv_estaragonrestringido ea
  WHERE ea.nominalsamplesnumber = 24;

-- Permissions

ALTER TABLE paper.gv_estaragon24restringido OWNER TO postgres;
GRANT ALL ON TABLE paper.gv_estaragon24restringido TO postgres;

-- paper.gv_aragoneseclimaticstations24data source

CREATE OR REPLACE VIEW paper.gv_aragoneseclimaticstations24data
AS SELECT dfc.fechacalculada AS fecha,
    dfc.hora,
        CASE
            WHEN to_char(dfc.horacalculada::interval, 'HH24:MI:SS'::text) = '00:00:00'::text THEN 0
            ELSE dfc.horamin
        END AS horamin,
    dfc.nombrecorto,
    dfc.idprovincia,
    dfc.idestacion,
    dfc.nombre,
    dfc.tempmedia,
    dfc.humedadmedia,
    dfc.velviento,
    dfc.dirviento,
    dfc.radiacion,
    dfc.precipitacion,
    dfc.tempmediacaja,
    dfc.tempsuelo1,
    dfc.tempsuelo2,
    dfc.codtempsuelo2,
    dfc.humhoja,
    dfc.presion,
    dfc.inso,
    dfc.huso,
    dfc.xutm,
    dfc.yutm,
    dfc.latitude,
    dfc.longitude,
    dfc.altitud,
    dfc.nominalsamplesnumber,
    dfc.timestampcalculado
   FROM ( SELECT ah.fecha,
            ah.hora,
                CASE
                    WHEN ah.hora = '00:00:00'::time without time zone AND ah.horamin = 1440 THEN 0
                    WHEN ah.hora = '00:00:00'::time without time zone AND ah.horamin = 2400 THEN 0
                    ELSE ah.horamin
                END AS horamin,
            ah.indicativo AS nombrecorto,
            e.idprovincia,
            e.id AS idestacion,
            e.nombre,
            ah.tmed AS tempmedia,
            ah.hr AS humedadmedia,
            ah.vv AS velviento,
            ah.dv AS dirviento,
            ah.rad AS radiacion,
            ah.precip AS precipitacion,
            NULL::text AS tempmediacaja,
            ah.t30 AS tempsuelo1,
            NULL::text AS tempsuelo2,
            NULL::text AS codtempsuelo2,
            ah.humhoja,
            ah.presion,
            ah.inso,
            e.huso,
            e.utmx AS xutm,
            e.utmy AS yutm,
            e.lat AS latitude,
            e.long AS longitude,
            e.altitud,
            e.nominalsamplesnumber,
            ah.fecha + (((ah.horamin / 100 * 60 + mod(ah.horamin, 100)) || 'minutes'::text)::interval) AS timestampcalculado,
            (ah.fecha + (((ah.horamin / 100 * 60 + mod(ah.horamin, 100)) || 'minutes'::text)::interval))::date AS fechacalculada,
            (ah.fecha + (((ah.horamin / 100 * 60 + mod(ah.horamin, 100)) || 'minutes'::text)::interval))::time without time zone AS horacalculada
           FROM ( SELECT ah1.indicativo,
                    ah1.ubi,
                    ah1."año",
                    ah1.dia,
                    ah1.fecha,
                    ah1.horamin,
                    ah1.tmed,
                    ah1.hr,
                    ah1.precip,
                    ah1.vv,
                    ah1.dv,
                    ah1.humhoja,
                    ah1.presion,
                    ah1.rad,
                    ah1.h30,
                    ah1.t30,
                    ah1.inso,
                    ah1.bat,
                    ah1.hora
                   FROM paper.arahorario ah1
                  WHERE ah1.fecha >= to_timestamp('2008-09-01'::text, 'yyyy-mm-dd'::text)) ah
             JOIN paper.gv_estaragon24restringido e ON ah.indicativo::text = e.indicativo::text) dfc;

-- Permissions

ALTER TABLE paper.gv_aragoneseclimaticstations24data OWNER TO postgres;
GRANT ALL ON TABLE paper.gv_aragoneseclimaticstations24data TO postgres;




CREATE OR REPLACE VIEW paper.gv_historicalestimationdate
AS SELECT '2022-01-01'::date AS refdate;

-- Permissions

ALTER TABLE paper.gv_historicalestimationdate OWNER TO postgres;
GRANT ALL ON TABLE paper.gv_historicalestimationdate TO postgres;



-- paper.gv_aragoneseclimaticstationsdatafirstrecord source

CREATE OR REPLACE VIEW paper.gv_aragoneseclimaticstationsdatafirstrecord
AS SELECT f.fecha,
    f.hora,
    f.horamin,
    f.nombrecorto,
    f.idprovincia,
    f.idestacion,
    f.nombre,
    f.tempmedia,
    f.humedadmedia,
    f.velviento,
    f.dirviento,
    f.radiacion,
    f.precipitacion,
    f.tempmediacaja,
    f.tempsuelo1,
    f.tempsuelo2,
    f.codtempsuelo2,
    f.humhoja,
    f.presion,
    f.inso,
    f.huso,
    f.xutm,
    f.yutm,
    f.latitude,
    f.longitude,
    f.altitud,
    f.timestampcalculado
   FROM ( SELECT d.fecha,
            d.hora,
            d.horamin,
            d.nombrecorto,
            d.idprovincia,
            d.idestacion,
            d.nombre,
            d.tempmedia,
            d.humedadmedia,
            d.velviento,
            d.dirviento,
            d.radiacion,
            d.precipitacion,
            d.tempmediacaja,
            d.tempsuelo1,
            d.tempsuelo2,
            d.codtempsuelo2,
            d.humhoja,
            d.presion,
            d.inso,
            d.huso,
            d.xutm,
            d.yutm,
            d.latitude,
            d.longitude,
            d.altitud,
            d.nominalsamplesnumber,
            d.timestampcalculado
           FROM paper.gv_aragoneseclimaticstationsdata d
             JOIN ( SELECT a.nombrecorto,
                    min(a.timestampcalculado) AS timestampcalculado
                   FROM paper.gv_aragoneseclimaticstationsdata a
                  GROUP BY a.nombrecorto) e ON upper(d.nombrecorto::text) = upper(e.nombrecorto::text) AND d.timestampcalculado = e.timestampcalculado) f;

-- Permissions

ALTER TABLE paper.gv_aragoneseclimaticstationsdatafirstrecord OWNER TO postgres;
GRANT ALL ON TABLE paper.gv_aragoneseclimaticstationsdatafirstrecord TO postgres;



-- paper.gen_firststationsampledate definition

-- Drop table

-- DROP TABLE paper.gen_firststationsampledate;

CREATE TABLE paper.gen_firststationsampledate (
	fecha date NOT NULL,
	hora time NULL,
	horamin int4 NOT NULL,
	nombrecorto varchar(6) NULL,
	idprovincia int4 NULL,
	idestacion int4 NOT NULL,
	nombre varchar(254) NULL,
	tempmedia numeric(6, 2) NULL,
	humedadmedia numeric(6, 2) NULL,
	velviento numeric(6, 2) NULL,
	dirviento int4 NULL,
	radiacion numeric(6, 2) NULL,
	precipitacion numeric(6, 2) NULL,
	tempmediacaja numeric(6, 2) NULL,
	tempsuelo1 numeric(6, 2) NULL,
	tempsuelo2 numeric(6, 2) NULL,
	codtempsuelo2 numeric(6, 2) NULL,
	humhoja numeric(6, 2) NULL,
	presion numeric(6, 2) NULL,
	inso numeric(6, 2) NULL,
	huso int8 NULL,
	xutm int8 NULL,
	yutm int8 NULL,
	latitude numeric(10, 6) NULL,
	longitude numeric(10, 6) NULL,
	altitud float8 NULL,
	timestampcalculado timestamp NULL,
	CONSTRAINT gen_firststationsampledate_pkey PRIMARY KEY (idestacion)
);

-- Permissions

ALTER TABLE paper.gen_firststationsampledate OWNER TO postgres;
GRANT ALL ON TABLE paper.gen_firststationsampledate TO postgres;




-- paper.gv_staticaragoneseclimaticstations24datafirstrecord source

CREATE OR REPLACE VIEW paper.gv_staticaragoneseclimaticstations24datafirstrecord
AS SELECT fsd.fecha,
    fsd.hora,
    fsd.horamin,
    fsd.nombrecorto,
    fsd.idprovincia,
    fsd.idestacion,
    fsd.nombre,
    fsd.tempmedia,
    fsd.humedadmedia,
    fsd.velviento,
    fsd.dirviento,
    fsd.radiacion,
    fsd.precipitacion,
    fsd.tempmediacaja,
    fsd.tempsuelo1,
    fsd.tempsuelo2,
    fsd.codtempsuelo2,
    fsd.humhoja,
    fsd.presion,
    fsd.inso,
    fsd.huso,
    fsd.xutm,
    fsd.yutm,
    fsd.latitude,
    fsd.longitude,
    fsd.altitud,
    fsd.timestampcalculado
   FROM paper.gen_firststationsampledate fsd
     JOIN paper.gv_estaragon24restringido ge ON fsd.idestacion = ge.id;

-- Permissions

ALTER TABLE paper.gv_staticaragoneseclimaticstations24datafirstrecord OWNER TO postgres;
GRANT ALL ON TABLE paper.gv_staticaragoneseclimaticstations24datafirstrecord TO postgres;


-- paper.gv_estaragon48restringido source

CREATE OR REPLACE VIEW paper.gv_estaragon48restringido
AS SELECT ea.id,
    ea.geom,
    ea.indicativo,
    ea.nombre,
    ea.provincia,
    ea.altitud,
    ea.datum,
    ea.idestacion,
    ea.ubi,
    ea.idprovincia,
    ea.huso,
    ea.idtermino,
    ea.nombrecorto,
    ea.estado,
    ea.layer,
    ea.utmx,
    ea.utmy,
    ea.alt,
    ea.long,
    ea.lat,
    ea.considered,
    ea.location,
    ea.pocid,
    ea.nominalsamplesnumber
   FROM paper.gv_estaragonrestringido ea
  WHERE ea.nominalsamplesnumber = 48;

-- Permissions

ALTER TABLE paper.gv_estaragon48restringido OWNER TO postgres;
GRANT ALL ON TABLE paper.gv_estaragon48restringido TO postgres;



CREATE OR REPLACE VIEW paper.gv_staticaragoneseclimaticstations48datafirstrecord
AS SELECT fsd.fecha,
    fsd.hora,
    fsd.horamin,
    fsd.nombrecorto,
    fsd.idprovincia,
    fsd.idestacion,
    fsd.nombre,
    fsd.tempmedia,
    fsd.humedadmedia,
    fsd.velviento,
    fsd.dirviento,
    fsd.radiacion,
    fsd.precipitacion,
    fsd.tempmediacaja,
    fsd.tempsuelo1,
    fsd.tempsuelo2,
    fsd.codtempsuelo2,
    fsd.humhoja,
    fsd.presion,
    fsd.inso,
    fsd.huso,
    fsd.xutm,
    fsd.yutm,
    fsd.latitude,
    fsd.longitude,
    fsd.altitud,
    fsd.timestampcalculado
   FROM paper.gen_firststationsampledate fsd
     JOIN paper.gv_estaragon48restringido ge ON fsd.idestacion = ge.id;

-- Permissions

ALTER TABLE paper.gv_staticaragoneseclimaticstations48datafirstrecord OWNER TO postgres;
GRANT ALL ON TABLE paper.gv_staticaragoneseclimaticstations48datafirstrecord TO postgres;

-- paper.calendario definition

-- Drop table

-- DROP TABLE paper.calendario;

CREATE TABLE paper.calendario (
	id int4 NULL,
	"date" date NULL
);

-- Permissions

ALTER TABLE paper.calendario OWNER TO postgres;
GRANT ALL ON TABLE paper.calendario TO postgres;


-- paper.gv_aragoneseclimaticstations48data source

CREATE OR REPLACE VIEW paper.gv_aragoneseclimaticstations48data
AS SELECT dfc.fechacalculada AS fecha,
    dfc.hora,
        CASE
            WHEN to_char(dfc.horacalculada::interval, 'HH24:MI:SS'::text) = '00:00:00'::text THEN 0
            ELSE dfc.horamin
        END AS horamin,
    dfc.nombrecorto,
    dfc.idprovincia,
    dfc.idestacion,
    dfc.nombre,
    dfc.tempmedia,
    dfc.humedadmedia,
    dfc.velviento,
    dfc.dirviento,
    dfc.radiacion,
    dfc.precipitacion,
    dfc.tempmediacaja,
    dfc.tempsuelo1,
    dfc.tempsuelo2,
    dfc.codtempsuelo2,
    dfc.humhoja,
    dfc.presion,
    dfc.inso,
    dfc.huso,
    dfc.xutm,
    dfc.yutm,
    dfc.latitude,
    dfc.longitude,
    dfc.altitud,
    dfc.nominalsamplesnumber,
    dfc.timestampcalculado
   FROM ( SELECT ah.fecha,
            ah.hora,
                CASE
                    WHEN ah.hora = '00:00:00'::time without time zone AND ah.horamin = 1440 THEN 0
                    WHEN ah.hora = '00:00:00'::time without time zone AND ah.horamin = 2400 THEN 0
                    ELSE ah.horamin
                END AS horamin,
            ah.indicativo AS nombrecorto,
            e.idprovincia,
            e.id AS idestacion,
            e.nombre,
            ah.tmed AS tempmedia,
            ah.hr AS humedadmedia,
            ah.vv AS velviento,
            ah.dv AS dirviento,
            ah.rad AS radiacion,
            ah.precip AS precipitacion,
            NULL::text AS tempmediacaja,
            ah.t30 AS tempsuelo1,
            NULL::text AS tempsuelo2,
            NULL::text AS codtempsuelo2,
            ah.humhoja,
            ah.presion,
            ah.inso,
            e.huso,
            e.utmx AS xutm,
            e.utmy AS yutm,
            e.lat AS latitude,
            e.long AS longitude,
            e.altitud,
            e.nominalsamplesnumber,
            ah.fecha + (((ah.horamin / 100 * 60 + mod(ah.horamin, 100)) || 'minutes'::text)::interval) AS timestampcalculado,
            (ah.fecha + (((ah.horamin / 100 * 60 + mod(ah.horamin, 100)) || 'minutes'::text)::interval))::date AS fechacalculada,
            (ah.fecha + (((ah.horamin / 100 * 60 + mod(ah.horamin, 100)) || 'minutes'::text)::interval))::time without time zone AS horacalculada
           FROM ( SELECT ah1.indicativo,
                    ah1.ubi,
                    ah1."año",
                    ah1.dia,
                    ah1.fecha,
                    ah1.horamin,
                    ah1.tmed,
                    ah1.hr,
                    ah1.precip,
                    ah1.vv,
                    ah1.dv,
                    ah1.humhoja,
                    ah1.presion,
                    ah1.rad,
                    ah1.h30,
                    ah1.t30,
                    ah1.inso,
                    ah1.bat,
                    ah1.hora
                   FROM arahorario ah1
                  WHERE ah1.fecha >= to_timestamp('2008-09-01'::text, 'yyyy-mm-dd'::text)) ah
             JOIN gv_estaragon48restringido e ON ah.indicativo::text = e.indicativo::text) dfc;

-- Permissions

ALTER TABLE paper.gv_aragoneseclimaticstations48data OWNER TO postgres;
GRANT ALL ON TABLE paper.gv_aragoneseclimaticstations48data TO postgres;





-- paper.gen_dateswithlesssamplesforstationsnew source

CREATE OR REPLACE VIEW paper.gen_dateswithlesssamplesforstationsnew
AS SELECT h.date,
    upper(h.indicativo) AS indicativo,
    h.samples
   FROM ( SELECT f.date,
            f.indicativo,
            f.samples
           FROM ( SELECT b.date,
                    b.indicativo,
                    count(d.*) AS samples
                   FROM ( SELECT DISTINCT c.id,
                            c.date,
                            ga.nombrecorto::text AS indicativo
                           FROM paper.calendario c,
                            paper.gv_staticaragoneseclimaticstations48datafirstrecord ga,
                            paper.gv_historicalestimationdate hed
                          WHERE c.date < (CURRENT_DATE - 1) AND c.date > ga.fecha AND c.date >= hed.refdate) b
                     LEFT JOIN ( SELECT upper(a.nombrecorto::text) AS indicativo,
                            a.nombre AS ubi,
                            date_part('year'::text, a.fecha) AS "año",
                            date_part('doy'::text, a.fecha) AS dia,
                            a.fecha,
                            a.horamin,
                            a.tempmedia AS tmed,
                            a.humedadmedia AS hr,
                            a.precipitacion AS precip,
                            a.velviento AS vv,
                            a.dirviento AS dv,
                            a.humhoja,
                            a.presion,
                            a.radiacion AS rad,
                            NULL::text AS h30,
                            a.tempsuelo1 AS t30,
                            a.inso,
                            NULL::text AS bat,
                            a.hora
                           FROM paper.gv_aragoneseclimaticstations48data a,
                            paper.gv_historicalestimationdate hed
                          WHERE a.fecha >= hed.refdate AND a.fecha < (CURRENT_DATE - 1)) d ON b.date = d.fecha AND upper(b.indicativo) = upper(d.indicativo)
                  GROUP BY b.date, b.indicativo) f
          WHERE f.samples < 48
        UNION ALL
         SELECT f.date,
            f.indicativo,
            f.samples
           FROM ( SELECT b.date,
                    b.indicativo,
                    count(d.*) AS samples
                   FROM ( SELECT DISTINCT c.id,
                            c.date,
                            ga.nombrecorto::text AS indicativo
                           FROM paper.calendario c,
                            paper.gv_staticaragoneseclimaticstations24datafirstrecord ga,
                            paper.gv_historicalestimationdate hed
                          WHERE c.date < (CURRENT_DATE - 1) AND c.date > ga.fecha AND c.date >= hed.refdate) b
                     LEFT JOIN ( SELECT upper(a.nombrecorto::text) AS indicativo,
                            a.nombre AS ubi,
                            date_part('year'::text, a.fecha) AS "año",
                            date_part('doy'::text, a.fecha) AS dia,
                            a.fecha,
                            a.horamin,
                            a.tempmedia AS tmed,
                            a.humedadmedia AS hr,
                            a.precipitacion AS precip,
                            a.velviento AS vv,
                            a.dirviento AS dv,
                            a.humhoja,
                            a.presion,
                            a.radiacion AS rad,
                            NULL::text AS h30,
                            a.tempsuelo1 AS t30,
                            a.inso,
                            NULL::text AS bat,
                            a.hora
                           FROM paper.gv_aragoneseclimaticstations24data a,
                            paper.gv_historicalestimationdate hed
                          WHERE a.fecha >= hed.refdate AND a.fecha < (CURRENT_DATE - 1)) d ON b.date = d.fecha AND upper(b.indicativo) = upper(d.indicativo)
                  GROUP BY b.date, b.indicativo) f
          WHERE f.samples < 24) h
  ORDER BY h.indicativo, h.date DESC;

-- Permissions

ALTER TABLE paper.gen_dateswithlesssamplesforstationsnew OWNER TO postgres;
GRANT ALL ON TABLE paper.gen_dateswithlesssamplesforstationsnew TO postgres;


-- paper.estacion_calendario_horas_minutos_faltan definition

-- Drop table

-- DROP TABLE paper.estacion_calendario_horas_minutos_faltan;

CREATE TABLE paper.estacion_calendario_horas_minutos_faltan (
	id int8 DEFAULT nextval('cadastral.agrupaciongeofrafica_id_seq'::regclass) NOT NULL,
	date_hour timestamp NOT NULL,
	"date" date NOT NULL,
	"hour" time DEFAULT '00:00:00'::time without time zone NOT NULL,
	indicativo varchar(254) NOT NULL,
	CONSTRAINT estacion_calendario_horas_minutos_faltan_pkey PRIMARY KEY (id)
);
CREATE INDEX estacion_calendario_horas_minutos_faltan_idx ON paper.estacion_calendario_horas_minutos_faltan USING btree (indicativo, date);
CREATE INDEX estacion_calendario_horas_minutos_faltan_idx2 ON paper.estacion_calendario_horas_minutos_faltan USING btree (indicativo, date_hour);

-- Permissions

ALTER TABLE paper.estacion_calendario_horas_minutos_faltan OWNER TO postgres;
GRANT ALL ON TABLE paper.estacion_calendario_horas_minutos_faltan TO postgres;


-- paper.gv_historicoestacioncalendariohoras_minutosfaltan source

CREATE OR REPLACE VIEW paper.gv_historicoestacioncalendariohoras_minutosfaltan
AS SELECT estacion_calendario_horas_minutos_faltan.id,
    estacion_calendario_horas_minutos_faltan.date_hour,
    estacion_calendario_horas_minutos_faltan.date,
    estacion_calendario_horas_minutos_faltan.hour,
    estacion_calendario_horas_minutos_faltan.indicativo,
    NULL::text AS samples
   FROM paper.estacion_calendario_horas_minutos_faltan,
    gv_historicalestimationdate hed
  WHERE estacion_calendario_horas_minutos_faltan.date < hed.refdate;

-- Permissions

ALTER TABLE paper.gv_historicoestacioncalendariohoras_minutosfaltan OWNER TO postgres;
GRANT ALL ON TABLE paper.gv_historicoestacioncalendariohoras_minutosfaltan TO postgres;



-- paper.gen_dateswithlesssamplesforstations source

CREATE OR REPLACE VIEW paper.gen_dateswithlesssamplesforstations
AS SELECT a.date,
    a.indicativo,
    a.samples
   FROM ( SELECT paper.gen_dateswithlesssamplesforstationsnew.date,
            paper.gen_dateswithlesssamplesforstationsnew.indicativo,
            paper.gen_dateswithlesssamplesforstationsnew.samples
           FROM paper.gen_dateswithlesssamplesforstationsnew
        UNION ALL
         SELECT DISTINCT paper.gv_historicoestacioncalendariohoras_minutosfaltan.date,
            paper.gv_historicoestacioncalendariohoras_minutosfaltan.indicativo,
            paper.gv_historicoestacioncalendariohoras_minutosfaltan.samples::integer AS samples
           FROM paper.gv_historicoestacioncalendariohoras_minutosfaltan) a
  ORDER BY a.indicativo, a.date;

-- Permissions

ALTER TABLE paper.gen_dateswithlesssamplesforstations OWNER TO postgres;
GRANT ALL ON TABLE paper.gen_dateswithlesssamplesforstations TO postgres;



-- paper.grapevine_stations_used definition

-- Drop table

-- DROP TABLE paper.grapevine_stations_used;

CREATE TABLE paper.grapevine_stations_used (
	"year" text NULL,
	station text NULL
);

-- Permissions

ALTER TABLE paper.grapevine_stations_used OWNER TO postgres;
GRANT INSERT, DELETE, UPDATE, REFERENCES, TRIGGER, TRUNCATE, SELECT ON TABLE paper.grapevine_stations_used TO postgres;

-- paper.siar_datoshorarios definition

-- Drop table

-- DROP TABLE paper.siar_datoshorarios;

CREATE TABLE paper.siar_datoshorarios (
	idprovincia int2 NOT NULL,
	idestacion varchar(10) NOT NULL,
	año int2 NOT NULL,
	dia int2 NOT NULL,
	fecha date NULL,
	horamin int2 NOT NULL,
	añosolar int2 NULL,
	diasolar int2 NULL,
	horaminsolar int2 NULL,
	tempmedia numeric(18, 2) NULL,
	codtempmedia int2 NULL,
	humedadmedia numeric(18, 2) NULL,
	codhumedadmedia int2 NULL,
	velviento numeric(18, 2) NULL,
	codvelviento int2 NULL,
	dirviento numeric(18, 2) NULL,
	coddirviento int2 NULL,
	radiacion numeric(18, 2) NULL,
	codradiacion int2 NULL,
	precipitacion numeric(18, 2) NULL,
	codprecipitacion int2 NULL,
	tempmediacaja numeric(18, 2) NULL,
	codtempmediacaja int2 NULL,
	fechaultmod date NULL,
	tempsuelo1 numeric(18, 2) NULL,
	codtempsuelo1 int2 NULL,
	tempsuelo2 numeric(18, 2) NULL,
	codtempsuelo2 int2 NULL,
	indicativo varchar NULL,
	CONSTRAINT restr_datoshorarios_2 PRIMARY KEY (idprovincia, idestacion, "año", dia, horamin)
);

-- Permissions

ALTER TABLE paper.siar_datoshorarios OWNER TO postgres;
GRANT ALL ON TABLE paper.siar_datoshorarios TO postgres;


-- paper.gv_aragoneseclimaticstationsdata source

CREATE OR REPLACE VIEW paper.gv_aragoneseclimaticstationsdata
AS SELECT dfc.fechacalculada AS fecha,
    dfc.hora,
        CASE
            WHEN to_char(dfc.horacalculada::interval, 'HH24:MI:SS'::text) = '00:00:00'::text THEN 0
            ELSE dfc.horamin::integer
        END AS horamin,
    dfc.nombrecorto::character varying(10) AS nombrecorto,
    dfc.idprovincia,
    dfc.idestacion::integer AS idestacion,
    dfc.nombre,
    dfc.tempmedia::numeric(6,2) AS tempmedia,
    dfc.humedadmedia::numeric(6,2) AS humedadmedia,
    dfc.velviento::numeric(6,2) AS velviento,
    dfc.dirviento::integer AS dirviento,
    dfc.radiacion::numeric(6,2) AS radiacion,
    dfc.precipitacion::numeric(6,2) AS precipitacion,
    dfc.tempmediacaja,
    dfc.tempsuelo1::numeric(6,2) AS tempsuelo1,
    dfc.tempsuelo2::text AS tempsuelo2,
    dfc.codtempsuelo2::text AS codtempsuelo2,
    dfc.humhoja::numeric(6,2) AS humhoja,
    dfc.presion::numeric(6,2) AS presion,
    dfc.inso::numeric(6,2) AS inso,
    dfc.huso,
    dfc.xutm,
    dfc.yutm,
    dfc.latitude,
    dfc.longitude,
    dfc.altitud,
    dfc.nominalsamplesnumber,
    dfc.timestampcalculado
   FROM ( SELECT e_1.indicativo AS nombrecorto,
            e_1.ubi,
            ah1."año",
            ah1.dia,
                CASE
                    WHEN ah1.horamin = 2400 THEN ah1.fecha + '1 day'::interval
                    ELSE ah1.fecha::timestamp without time zone
                END::date AS fecha,
                CASE
                    WHEN ah1.horamin = 2400 THEN 0
                    ELSE ah1.horamin::integer
                END::smallint AS horamin,
            ah1.tempmedia,
            ah1.humedadmedia,
            ah1.precipitacion,
            ah1.velviento,
            ah1.dirviento,
            NULL::text AS humhoja,
            NULL::text AS presion,
            ah1.radiacion,
            NULL::text AS h30,
            NULL::text AS tempmediacaja,
            ah1.tempsuelo1,
            ah1.tempsuelo2,
            ah1.codtempsuelo1,
            ah1.codtempsuelo2,
            NULL::text AS inso,
            NULL::text AS bat,
                CASE
                    WHEN ah1.horamin = 2400 THEN to_timestamp((0 || ':'::text) || mod(ah1.horamin::integer, 100), 'HH24:MI'::text)::time without time zone
                    ELSE to_timestamp(((ah1.horamin / 100) || ':'::text) || mod(ah1.horamin::integer, 100), 'HH24:MI'::text)::time without time zone
                END AS hora,
            e_1.huso,
            e_1.utmx AS xutm,
            e_1.utmy AS yutm,
            e_1.lat AS latitude,
            e_1.long AS longitude,
            e_1.altitud,
            e_1.nominalsamplesnumber,
            e_1.nombre,
            ah1.fecha + (((ah1.horamin / 100 * 60 + mod(ah1.horamin::integer, 100)) || 'minutes'::text)::interval) AS timestampcalculado,
            (ah1.fecha + (((ah1.horamin / 100 * 60 + mod(ah1.horamin::integer, 100)) || 'minutes'::text)::interval))::date AS fechacalculada,
            (ah1.fecha + (((ah1.horamin / 100 * 60 + mod(ah1.horamin::integer, 100)) || 'minutes'::text)::interval))::time without time zone AS horacalculada,
            e_1.idprovincia,
            e_1.idestacion
           FROM paper.siar_datoshorarios ah1
             JOIN paper.estaragon e_1 ON ah1.idprovincia = e_1.idprovincia AND ah1.idestacion::text = e_1.idestacion::text
          WHERE e_1.estado::text = 'Activa'::text AND (e_1.idprovincia = ANY (ARRAY[22, 44, 50]))
          ORDER BY ah1.fecha DESC, (
                CASE
                    WHEN ah1.horamin = 2400 THEN 0
                    ELSE ah1.horamin::integer
                END::smallint) DESC) dfc;

-- Permissions

ALTER TABLE paper.gv_aragoneseclimaticstationsdata OWNER TO postgres;
GRANT ALL ON TABLE paper.gv_aragoneseclimaticstationsdata TO postgres;

-- paper.gv_estaragonrestringido source

CREATE OR REPLACE VIEW paper.gv_estaragonrestringido
AS SELECT estaragon.id,
    estaragon.geom,
    estaragon.indicativo,
    estaragon.nombre,
    estaragon.provincia,
    estaragon.altitud,
    estaragon.datum,
    estaragon.idestacion,
    estaragon.ubi,
    estaragon.idprovincia,
    estaragon.huso,
    estaragon.idtermino,
    estaragon.nombrecorto,
    estaragon.estado,
    estaragon.layer,
    estaragon.utmx,
    estaragon.utmy,
    estaragon.alt,
    estaragon.long,
    estaragon.lat,
    estaragon.considered,
    estaragon.location,
    estaragon.pocid,
    estaragon.nominalsamplesnumber
   FROM paper.estaragon
  WHERE NOT estaragon.indicativo::text ~ '^[0-9].*'::text AND estaragon.indicativo::text <> 'Z28'::text AND estaragon.idestacion::text <> 'gv13baja'::text AND estaragon.considered;

-- Permissions

ALTER TABLE paper.gv_estaragonrestringido OWNER TO postgres;
GRANT ALL ON TABLE paper.gv_estaragonrestringido TO postgres;


-- paper.gv_estaragon source

CREATE OR REPLACE VIEW paper.gv_estaragon
AS SELECT estaragon.id,
    estaragon.geom,
    estaragon.indicativo,
    estaragon.nombre,
    estaragon.provincia,
    estaragon.altitud,
    estaragon.datum,
    estaragon.idestacion,
    estaragon.ubi,
    estaragon.idprovincia,
    estaragon.huso,
    estaragon.idtermino,
    estaragon.nombrecorto,
    estaragon.estado,
    estaragon.layer,
    estaragon.utmx,
    estaragon.utmy,
    estaragon.alt,
    estaragon.long,
    estaragon.lat,
    estaragon.considered,
    estaragon.location,
    estaragon.pocid,
    estaragon.nominalsamplesnumber
   FROM paper.estaragon;

-- Permissions

ALTER TABLE paper.gv_estaragon OWNER TO postgres;
GRANT ALL ON TABLE paper.gv_estaragon TO postgres;


-- paper.gv_estaragon48 source

CREATE OR REPLACE VIEW paper.gv_estaragon48
AS SELECT ea.id,
    ea.geom,
    ea.indicativo,
    ea.nombre,
    ea.provincia,
    ea.altitud,
    ea.datum,
    ea.idestacion,
    ea.ubi,
    ea.idprovincia,
    ea.huso,
    ea.idtermino,
    ea.nombrecorto,
    ea.estado,
    ea.layer,
    ea.utmx,
    ea.utmy,
    ea.alt,
    ea.long,
    ea.lat,
    ea.considered,
    ea.location,
    ea.pocid,
    ea.nominalsamplesnumber
   FROM paper.gv_estaragon ea
  WHERE ea.nominalsamplesnumber = 48 AND ea.considered;

-- Permissions

ALTER TABLE paper.gv_estaragon48 OWNER TO postgres;
GRANT ALL ON TABLE paper.gv_estaragon48 TO postgres;

-- paper.gv_recentestacioncalendariohoras_minutosfaltan source

CREATE OR REPLACE VIEW paper.gv_recentestacioncalendariohoras_minutosfaltan
AS SELECT estacion_calendario_horas_minutos_faltan.id,
    estacion_calendario_horas_minutos_faltan.date_hour,
    estacion_calendario_horas_minutos_faltan.date,
    estacion_calendario_horas_minutos_faltan.hour,
    estacion_calendario_horas_minutos_faltan.indicativo,
    NULL::text AS samples
   FROM paper.estacion_calendario_horas_minutos_faltan,
    paper.gv_historicalestimationdate hed
  WHERE estacion_calendario_horas_minutos_faltan.date >= hed.refdate;

-- Permissions

ALTER TABLE paper.gv_recentestacioncalendariohoras_minutosfaltan OWNER TO postgres;
GRANT ALL ON TABLE paper.gv_recentestacioncalendariohoras_minutosfaltan TO postgres;


CREATE OR REPLACE VIEW paper.gv_estimate_arahorario
AS SELECT a.id,
    a.indicativo,
    a.ubi,
    a."año",
    a.dia,
    a.fecha,
    a.horamin,
    a.tmed,
    a.hr,
    a.precip,
    a.vv,
    a.dv,
    a.humhoja,
    a.presion,
    a.rad,
    a.h30,
    a.t30,
    a.inso,
    a.bat,
    a.hora,
    a.stimationproviderid
   FROM public.estimate_arahorario a
     RIGHT JOIN ( SELECT max(estimate_arahorario.id) AS id,
            estimate_arahorario.indicativo,
            estimate_arahorario.fecha,
            estimate_arahorario.hora
           FROM public.estimate_arahorario
          GROUP BY estimate_arahorario.indicativo, estimate_arahorario.fecha, estimate_arahorario.hora) b ON a.id = b.id
  WHERE length(b.hora::text) <> 5
  ORDER BY a.fecha DESC, a.indicativo, a.hora;

-- Permissions

ALTER TABLE paper.gv_estimate_arahorario OWNER TO postgres;
GRANT ALL ON TABLE paper.gv_estimate_arahorario TO postgres;


-- paper.estimate_arahorario definition

-- Drop table

-- DROP TABLE paper.estimate_arahorario;

CREATE TABLE paper.estimate_arahorario (
	id int8 NOT NULL,
	indicativo varchar(10) NOT NULL,
	ubi varchar(50) NULL,
	año int4 NULL,
	dia int4 NULL,
	fecha date NOT NULL,
	horamin int4 NOT NULL,
	tmed numeric(6, 2) NULL,
	hr numeric(6, 2) NULL,
	precip numeric(6, 2) NULL,
	vv numeric(6, 2) NULL,
	dv int4 NULL,
	humhoja numeric(6, 2) NULL,
	presion numeric(6, 2) NULL,
	rad numeric(6, 2) NULL,
	h30 numeric(6, 2) NULL,
	t30 numeric(6, 2) NULL,
	inso numeric(6, 2) NULL,
	bat int4 NULL,
	hora varchar NULL,
	stimationproviderid int8 NOT NULL
);

-- Permissions

ALTER TABLE paper.estimate_arahorario OWNER TO postgres;
GRANT INSERT, DELETE, UPDATE, REFERENCES, TRIGGER, TRUNCATE, SELECT ON TABLE paper.estimate_arahorario TO postgres;



-- paper.gv_estimate_arahorario source

CREATE OR REPLACE VIEW paper.gv_estimate_arahorario
AS SELECT a.id,
    a.indicativo,
    a.ubi,
    a."año",
    a.dia,
    a.fecha,
    a.horamin,
    a.tmed,
    a.hr,
    a.precip,
    a.vv,
    a.dv,
    a.humhoja,
    a.presion,
    a.rad,
    a.h30,
    a.t30,
    a.inso,
    a.bat,
    a.hora,
    a.stimationproviderid
   FROM paper.estimate_arahorario a
     RIGHT JOIN ( SELECT max(estimate_arahorario.id) AS id,
            estimate_arahorario.indicativo,
            estimate_arahorario.fecha,
            estimate_arahorario.hora
           FROM paper.estimate_arahorario
          GROUP BY estimate_arahorario.indicativo, estimate_arahorario.fecha, estimate_arahorario.hora) b ON a.id = b.id
  WHERE length(b.hora::text) <> 5
  ORDER BY a.fecha DESC, a.indicativo, a.hora;

-- Permissions

ALTER TABLE paper.gv_estimate_arahorario OWNER TO postgres;
GRANT ALL ON TABLE paper.gv_estimate_arahorario TO postgres;
GRANT SELECT ON TABLE paper.gv_estimate_arahorario TO pistara;



-- paper.gv_estimatearahorariorecent source

CREATE OR REPLACE VIEW paper.gv_estimatearahorariorecent
AS SELECT estimate_arahorario.id,
    estimate_arahorario.indicativo,
    estimate_arahorario.ubi,
    estimate_arahorario."año",
    estimate_arahorario.dia,
    estimate_arahorario.fecha,
    date_part('hour'::text, estimate_arahorario.hora::time without time zone) * 100::double precision + date_part('minute'::text, estimate_arahorario.hora::time without time zone) AS horamin,
    estimate_arahorario.tmed,
    estimate_arahorario.hr,
    estimate_arahorario.precip,
    estimate_arahorario.vv,
    estimate_arahorario.dv,
    estimate_arahorario.humhoja,
    estimate_arahorario.presion,
    estimate_arahorario.rad,
    estimate_arahorario.h30,
    estimate_arahorario.t30,
    estimate_arahorario.inso,
    estimate_arahorario.bat,
    estimate_arahorario.hora,
    estimate_arahorario.stimationproviderid,
    estimate_arahorario.fecha + estimate_arahorario.hora::time without time zone AS timestampcalculado
   FROM paper.gv_estimate_arahorario estimate_arahorario,
    paper.gv_historicalestimationdate hed
  WHERE estimate_arahorario.fecha >= hed.refdate;

-- Permissions

ALTER TABLE paper.gv_estimatearahorariorecent OWNER TO postgres;
GRANT ALL ON TABLE paper.gv_estimatearahorariorecent TO postgres;


-- paper.gv_recentdatosestacionesreemplazar source

CREATE OR REPLACE VIEW paper.gv_recentdatosestacionesreemplazar
AS SELECT DISTINCT eah.indicativo,
    eah.ubi,
    eah."año",
    eah.dia,
    eah.fecha,
    eah.horamin,
    eah.tmed,
    eah.hr,
    eah.precip,
    eah.vv,
    eah.dv,
    eah.humhoja,
    eah.presion,
    eah.rad,
    eah.h30,
    eah.t30,
    eah.inso,
    eah.bat,
    eah.hora,
    eah.stimationproviderid,
    eah.timestampcalculado
   FROM paper.gv_recentestacioncalendariohoras_minutosfaltan jeclhmf
     JOIN paper.gv_estimatearahorariorecent eah ON upper(jeclhmf.indicativo::text) = upper(eah.indicativo::text) AND jeclhmf.date_hour = eah.timestampcalculado;

-- Permissions

ALTER TABLE paper.gv_recentdatosestacionesreemplazar OWNER TO postgres;
GRANT ALL ON TABLE paper.gv_recentdatosestacionesreemplazar TO postgres;

CREATE OR REPLACE VIEW paper.gv_estimatearahorariohistorico
AS SELECT estimate_arahorario.id,
    estimate_arahorario.indicativo,
    estimate_arahorario.ubi,
    estimate_arahorario."año",
    estimate_arahorario.dia,
    estimate_arahorario.fecha,
    date_part('hour'::text, estimate_arahorario.hora::time without time zone) * 100::double precision + date_part('minute'::text, estimate_arahorario.hora::time without time zone) AS horamin,
    estimate_arahorario.tmed,
    estimate_arahorario.hr,
    estimate_arahorario.precip,
    estimate_arahorario.vv,
    estimate_arahorario.dv,
    estimate_arahorario.humhoja,
    estimate_arahorario.presion,
    estimate_arahorario.rad,
    estimate_arahorario.h30,
    estimate_arahorario.t30,
    estimate_arahorario.inso,
    estimate_arahorario.bat,
    estimate_arahorario.hora,
    estimate_arahorario.stimationproviderid,
    estimate_arahorario.fecha + estimate_arahorario.hora::time without time zone AS timestampcalculado
   FROM paper.estimate_arahorario,
    paper.gv_historicalestimationdate hed
  WHERE estimate_arahorario.fecha < hed.refdate;

-- Permissions

ALTER TABLE paper.gv_estimatearahorariohistorico OWNER TO postgres;
GRANT ALL ON TABLE paper.gv_estimatearahorariohistorico TO postgres;

-- paper.gv_historicodatosestacionesreemplazar source

CREATE OR REPLACE VIEW paper.gv_historicodatosestacionesreemplazar
AS SELECT DISTINCT eah.indicativo,
    eah.ubi,
    eah."año",
    eah.dia,
    eah.fecha,
    eah.horamin,
    eah.tmed,
    eah.hr,
    eah.precip,
    eah.vv,
    eah.dv,
    eah.humhoja,
    eah.presion,
    eah.rad,
    eah.h30,
    eah.t30,
    eah.inso,
    eah.bat,
    eah.hora,
    eah.stimationproviderid,
    eah.timestampcalculado
   FROM paper.gv_historicoestacioncalendariohoras_minutosfaltan jeclhmf
     JOIN paper.gv_estimatearahorariohistorico eah ON upper(jeclhmf.indicativo::text) = upper(eah.indicativo::text) AND jeclhmf.date_hour = eah.timestampcalculado;

-- Permissions

ALTER TABLE paper.gv_historicodatosestacionesreemplazar OWNER TO postgres;
GRANT ALL ON TABLE paper.gv_historicodatosestacionesreemplazar TO postgres;



-- paper.gv_datosestacionesreemplazar source

CREATE OR REPLACE VIEW paper.gv_datosestacionesreemplazar
AS SELECT DISTINCT a.indicativo,
    a.ubi,
    a."año",
    a.dia,
    a.fecha,
    a.horamin,
    a.hora,
    a.tmed,
    a.hr,
    a.precip,
    a.vv,
    a.dv,
    a.humhoja,
    a.presion,
    a.rad,
    a.h30,
    a.t30,
    a.inso,
    a.bat,
    a.stimationproviderid,
    a.timestampcalculado
   FROM ( SELECT gv_recentdatosestacionesreemplazar.indicativo,
            gv_recentdatosestacionesreemplazar.ubi,
            gv_recentdatosestacionesreemplazar."año",
            gv_recentdatosestacionesreemplazar.dia,
            gv_recentdatosestacionesreemplazar.fecha,
            gv_recentdatosestacionesreemplazar.horamin,
            gv_recentdatosestacionesreemplazar.tmed,
            gv_recentdatosestacionesreemplazar.hr,
            gv_recentdatosestacionesreemplazar.precip,
            gv_recentdatosestacionesreemplazar.vv,
            gv_recentdatosestacionesreemplazar.dv,
            gv_recentdatosestacionesreemplazar.humhoja,
            gv_recentdatosestacionesreemplazar.presion,
            gv_recentdatosestacionesreemplazar.rad,
            gv_recentdatosestacionesreemplazar.h30,
            gv_recentdatosestacionesreemplazar.t30,
            gv_recentdatosestacionesreemplazar.inso,
            gv_recentdatosestacionesreemplazar.bat,
            gv_recentdatosestacionesreemplazar.hora,
            gv_recentdatosestacionesreemplazar.stimationproviderid,
            gv_recentdatosestacionesreemplazar.timestampcalculado
           FROM paper.gv_recentdatosestacionesreemplazar
        UNION ALL
         SELECT gv_historicodatosestacionesreemplazar.indicativo,
            gv_historicodatosestacionesreemplazar.ubi,
            gv_historicodatosestacionesreemplazar."año",
            gv_historicodatosestacionesreemplazar.dia,
            gv_historicodatosestacionesreemplazar.fecha,
            gv_historicodatosestacionesreemplazar.horamin,
            gv_historicodatosestacionesreemplazar.tmed,
            gv_historicodatosestacionesreemplazar.hr,
            gv_historicodatosestacionesreemplazar.precip,
            gv_historicodatosestacionesreemplazar.vv,
            gv_historicodatosestacionesreemplazar.dv,
            gv_historicodatosestacionesreemplazar.humhoja,
            gv_historicodatosestacionesreemplazar.presion,
            gv_historicodatosestacionesreemplazar.rad,
            gv_historicodatosestacionesreemplazar.h30,
            gv_historicodatosestacionesreemplazar.t30,
            gv_historicodatosestacionesreemplazar.inso,
            gv_historicodatosestacionesreemplazar.bat,
            gv_historicodatosestacionesreemplazar.hora,
            gv_historicodatosestacionesreemplazar.stimationproviderid,
            gv_historicodatosestacionesreemplazar.timestampcalculado
           FROM paper.gv_historicodatosestacionesreemplazar) a;

-- Permissions

ALTER TABLE paper.gv_datosestacionesreemplazar OWNER TO postgres;
GRANT ALL ON TABLE paper.gv_datosestacionesreemplazar TO postgres;


-- paper.gv_datosestacionesreemplazarhormin source

CREATE OR REPLACE VIEW paper.gv_datosestacionesreemplazarhormin
AS SELECT DISTINCT a.fecha,
    a.hora::time without time zone AS hora,
    a.horamin::integer AS horamin,
        CASE
            WHEN ge.nombrecorto::text ~~ 'GV%'::text THEN lower(ge.nombrecorto::text)::character varying
            ELSE ge.nombrecorto
        END AS nombrecorto,
    ge.idprovincia,
    ge.id AS idestacion,
    ge.nombre,
    a.tmed AS tempmedia,
    a.hr AS humedadmedia,
    a.vv AS velviento,
    a.dv AS dirviento,
    a.rad AS radiacion,
    a.precip AS precipitacion,
    NULL::text AS tempmediacaja,
    a.h30 AS tempsuelo1,
    a.t30 AS tempsuelo2,
    NULL::text AS codtempsuelo2,
    a.humhoja,
    a.presion,
    a.inso,
    ge.huso,
    ge.utmx AS xutm,
    ge.utmy AS yutm,
    ge.lat AS latitude,
    ge.long AS longitude,
    ge.altitud AS altitude,
    a.timestampcalculado
   FROM paper.gv_datosestacionesreemplazar a
     JOIN paper.gv_estaragonrestringido ge ON upper(a.indicativo::text) = upper(ge.indicativo::text);

-- Permissions

ALTER TABLE paper.gv_datosestacionesreemplazarhormin OWNER TO postgres;
GRANT ALL ON TABLE paper.gv_datosestacionesreemplazarhormin TO postgres;

-- paper.gv_aragoneseclimaticstationsdatawithforecastandestimatedata source

CREATE OR REPLACE VIEW paper.gv_aragoneseclimaticstationsdatawithforecastandestimatedata
AS SELECT dwf.fecha,
    dwf.hora,
    dwf.horamin,
    dwf.nombrecorto::text AS nombrecorto,
    dwf.idprovincia,
    dwf.idestacion,
    dwf.nombre,
    dwf.tempmedia,
    dwf.humedadmedia,
    dwf.velviento,
    dwf.dirviento,
    dwf.radiacion,
    dwf.precipitacion,
    dwf.tempmediacaja,
    dwf.tempsuelo1,
    dwf.tempsuelo2,
    dwf.codtempsuelo2,
    dwf.humhoja,
    dwf.presion,
    dwf.inso,
    dwf.huso,
    dwf.xutm,
    dwf.yutm,
    dwf.latitude,
    dwf.longitude,
    dwf.altitud,
    dwf.datatimestamp
   FROM ( SELECT cd.fecha,
            cd.hora,
            cd.horamin,
            cd.nombrecorto,
            cd.idprovincia,
            cd.idestacion,
            cd.nombre,
            cd.tempmedia,
            cd.humedadmedia,
            cd.velviento,
            cd.dirviento,
            cd.radiacion,
            cd.precipitacion,
            cd.tempmediacaja,
            cd.tempsuelo1,
            cd.tempsuelo2,
            cd.codtempsuelo2,
            cd.humhoja,
            cd.presion,
            cd.inso,
            cd.huso,
            cd.xutm,
            cd.yutm,
            cd.latitude,
            cd.longitude,
            cd.altitud,
            cd.timestampcalculado AS datatimestamp
           FROM paper.gv_aragoneseclimaticstationsdata2consider cd
        UNION ALL
         SELECT gv_forecastdata2considerstations.fecha,
            gv_forecastdata2considerstations.hora,
            gv_forecastdata2considerstations.horamin,
                CASE
                    WHEN gv_forecastdata2considerstations.nombrecorto::text ~~ 'GV%'::text THEN lower(gv_forecastdata2considerstations.nombrecorto::text)::character varying
                    ELSE gv_forecastdata2considerstations.nombrecorto
                END AS nombrecorto,
            gv_forecastdata2considerstations.idprovincia,
            gv_forecastdata2considerstations.idestacion,
            gv_forecastdata2considerstations.nombre,
            gv_forecastdata2considerstations.tempmedia,
            gv_forecastdata2considerstations.humedadmedia,
            gv_forecastdata2considerstations.velviento,
            gv_forecastdata2considerstations.dirviento,
            gv_forecastdata2considerstations.radiacion,
            gv_forecastdata2considerstations.precipitacion,
            NULL::text AS tempmediacaja,
            gv_forecastdata2considerstations.tempsuelo1,
            NULL::text AS tempsuelo2,
            NULL::text AS codtempsuelo2,
            gv_forecastdata2considerstations.humhoja,
            gv_forecastdata2considerstations.presion,
            gv_forecastdata2considerstations.inso,
            gv_forecastdata2considerstations.huso,
            gv_forecastdata2considerstations.xutm,
            gv_forecastdata2considerstations.yutm,
            gv_forecastdata2considerstations.latitude,
            gv_forecastdata2considerstations.longitude,
            gv_forecastdata2considerstations.altitud,
            gv_forecastdata2considerstations.forecasttimestamp AS datatimestamp
           FROM paper.gv_forecastdata2considerstations
        UNION ALL
         SELECT a.fecha,
            a.hora,
            a.horamin,
            a.nombrecorto,
            a.idprovincia,
            a.idestacion,
            a.nombre,
            a.tempmedia,
            a.humedadmedia,
            a.velviento,
            a.dirviento,
            a.radiacion,
            a.precipitacion,
            a.tempmediacaja,
            a.tempsuelo1,
            a.tempsuelo2::text AS tempsuelo2,
            a.codtempsuelo2,
            a.humhoja,
            a.presion,
            a.inso,
            a.huso,
            a.xutm,
            a.yutm,
            a.latitude,
            a.longitude,
            a.altitude,
            a.timestampcalculado AS datatimestamp
           FROM paper.gv_datosestacionesreemplazarhormin a) dwf;

-- Permissions

ALTER TABLE paper.gv_aragoneseclimaticstationsdatawithforecastandestimatedata OWNER TO postgres;
GRANT ALL ON TABLE paper.gv_aragoneseclimaticstationsdatawithforecastandestimatedata TO postgres;


-- paper.gv_aragoneseclimaticstationsdatawithforecastandestimatedata2 source

CREATE OR REPLACE VIEW paper.gv_aragoneseclimaticstationsdatawithforecastandestimatedata2
AS SELECT gv_aragoneseclimaticstationsdatawithforecastandestimatedata.fecha,
        CASE
            WHEN mod(gv_aragoneseclimaticstationsdatawithforecastandestimatedata.horamin::integer, 100) = 10 THEN concat(((gv_aragoneseclimaticstationsdatawithforecastandestimatedata.horamin::integer - mod(gv_aragoneseclimaticstationsdatawithforecastandestimatedata.horamin::integer, 100)) / 100)::character varying, ':00:00')::time without time zone
            WHEN mod(gv_aragoneseclimaticstationsdatawithforecastandestimatedata.horamin::integer, 100) = 50 THEN concat(((gv_aragoneseclimaticstationsdatawithforecastandestimatedata.horamin::integer - mod(gv_aragoneseclimaticstationsdatawithforecastandestimatedata.horamin::integer, 100)) / 100)::character varying, ':30:00')::time without time zone
            ELSE gv_aragoneseclimaticstationsdatawithforecastandestimatedata.hora
        END AS hora,
        CASE
            WHEN mod(gv_aragoneseclimaticstationsdatawithforecastandestimatedata.horamin::integer, 100) = 10 THEN (gv_aragoneseclimaticstationsdatawithforecastandestimatedata.horamin::integer - mod(gv_aragoneseclimaticstationsdatawithforecastandestimatedata.horamin::integer, 100))::double precision
            WHEN mod(gv_aragoneseclimaticstationsdatawithforecastandestimatedata.horamin::integer, 100) = 50 THEN (gv_aragoneseclimaticstationsdatawithforecastandestimatedata.horamin::integer - mod(gv_aragoneseclimaticstationsdatawithforecastandestimatedata.horamin::integer, 100) + 30)::double precision
            ELSE gv_aragoneseclimaticstationsdatawithforecastandestimatedata.horamin
        END AS horamin,
    gv_aragoneseclimaticstationsdatawithforecastandestimatedata.nombrecorto,
    gv_aragoneseclimaticstationsdatawithforecastandestimatedata.idprovincia,
    gv_aragoneseclimaticstationsdatawithforecastandestimatedata.idestacion,
    gv_aragoneseclimaticstationsdatawithforecastandestimatedata.nombre,
    gv_aragoneseclimaticstationsdatawithforecastandestimatedata.tempmedia,
    gv_aragoneseclimaticstationsdatawithforecastandestimatedata.humedadmedia,
    gv_aragoneseclimaticstationsdatawithforecastandestimatedata.velviento,
    gv_aragoneseclimaticstationsdatawithforecastandestimatedata.dirviento,
    gv_aragoneseclimaticstationsdatawithforecastandestimatedata.radiacion,
    gv_aragoneseclimaticstationsdatawithforecastandestimatedata.precipitacion,
    gv_aragoneseclimaticstationsdatawithforecastandestimatedata.tempmediacaja,
    gv_aragoneseclimaticstationsdatawithforecastandestimatedata.tempsuelo1,
    gv_aragoneseclimaticstationsdatawithforecastandestimatedata.tempsuelo2,
    gv_aragoneseclimaticstationsdatawithforecastandestimatedata.codtempsuelo2,
    gv_aragoneseclimaticstationsdatawithforecastandestimatedata.humhoja,
    gv_aragoneseclimaticstationsdatawithforecastandestimatedata.presion,
    gv_aragoneseclimaticstationsdatawithforecastandestimatedata.inso,
    gv_aragoneseclimaticstationsdatawithforecastandestimatedata.huso,
    gv_aragoneseclimaticstationsdatawithforecastandestimatedata.xutm,
    gv_aragoneseclimaticstationsdatawithforecastandestimatedata.yutm,
    gv_aragoneseclimaticstationsdatawithforecastandestimatedata.latitude,
    gv_aragoneseclimaticstationsdatawithforecastandestimatedata.longitude,
    gv_aragoneseclimaticstationsdatawithforecastandestimatedata.altitud,
    gv_aragoneseclimaticstationsdatawithforecastandestimatedata.datatimestamp
   FROM paper.gv_aragoneseclimaticstationsdatawithforecastandestimatedata;

-- Permissions

ALTER TABLE paper.gv_aragoneseclimaticstationsdatawithforecastandestimatedata2 OWNER TO postgres;
GRANT ALL ON TABLE paper.gv_aragoneseclimaticstationsdatawithforecastandestimatedata2 TO postgres;


-- paper.gv_aragoneseclimaticstationsdata source

CREATE OR REPLACE VIEW paper.gv_aragoneseclimaticstationsdata
AS SELECT dfc.fechacalculada AS fecha,
    dfc.hora,
        CASE
            WHEN to_char(dfc.horacalculada::interval, 'HH24:MI:SS'::text) = '00:00:00'::text THEN 0
            ELSE dfc.horamin::integer
        END AS horamin,
    dfc.nombrecorto::character varying(10) AS nombrecorto,
    dfc.idprovincia,
    dfc.idestacion::integer AS idestacion,
    dfc.nombre,
    dfc.tempmedia::numeric(6,2) AS tempmedia,
    dfc.humedadmedia::numeric(6,2) AS humedadmedia,
    dfc.velviento::numeric(6,2) AS velviento,
    dfc.dirviento::integer AS dirviento,
    dfc.radiacion::numeric(6,2) AS radiacion,
    dfc.precipitacion::numeric(6,2) AS precipitacion,
    dfc.tempmediacaja,
    dfc.tempsuelo1::numeric(6,2) AS tempsuelo1,
    dfc.tempsuelo2::text AS tempsuelo2,
    dfc.codtempsuelo2::text AS codtempsuelo2,
    dfc.humhoja::numeric(6,2) AS humhoja,
    dfc.presion::numeric(6,2) AS presion,
    dfc.inso::numeric(6,2) AS inso,
    dfc.huso,
    dfc.xutm,
    dfc.yutm,
    dfc.latitude,
    dfc.longitude,
    dfc.altitud,
    dfc.nominalsamplesnumber,
    dfc.timestampcalculado
   FROM ( SELECT e_1.indicativo AS nombrecorto,
            e_1.ubi,
            ah1."año",
            ah1.dia,
                CASE
                    WHEN ah1.horamin = 2400 THEN ah1.fecha + '1 day'::interval
                    ELSE ah1.fecha::timestamp without time zone
                END::date AS fecha,
                CASE
                    WHEN ah1.horamin = 2400 THEN 0
                    ELSE ah1.horamin::integer
                END::smallint AS horamin,
            ah1.tempmedia,
            ah1.humedadmedia,
            ah1.precipitacion,
            ah1.velviento,
            ah1.dirviento,
            NULL::text AS humhoja,
            NULL::text AS presion,
            ah1.radiacion,
            NULL::text AS h30,
            NULL::text AS tempmediacaja,
            ah1.tempsuelo1,
            ah1.tempsuelo2,
            ah1.codtempsuelo1,
            ah1.codtempsuelo2,
            NULL::text AS inso,
            NULL::text AS bat,
                CASE
                    WHEN ah1.horamin = 2400 THEN to_timestamp((0 || ':'::text) || mod(ah1.horamin::integer, 100), 'HH24:MI'::text)::time without time zone
                    ELSE to_timestamp(((ah1.horamin / 100) || ':'::text) || mod(ah1.horamin::integer, 100), 'HH24:MI'::text)::time without time zone
                END AS hora,
            e_1.huso,
            e_1.utmx AS xutm,
            e_1.utmy AS yutm,
            e_1.lat AS latitude,
            e_1.long AS longitude,
            e_1.altitud,
            e_1.nominalsamplesnumber,
            e_1.nombre,
            ah1.fecha + (((ah1.horamin / 100 * 60 + mod(ah1.horamin::integer, 100)) || 'minutes'::text)::interval) AS timestampcalculado,
            (ah1.fecha + (((ah1.horamin / 100 * 60 + mod(ah1.horamin::integer, 100)) || 'minutes'::text)::interval))::date AS fechacalculada,
            (ah1.fecha + (((ah1.horamin / 100 * 60 + mod(ah1.horamin::integer, 100)) || 'minutes'::text)::interval))::time without time zone AS horacalculada,
            e_1.idprovincia,
            e_1.idestacion
           FROM paper.siar_datoshorarios ah1
             JOIN paper.estaragon e_1 ON ah1.idprovincia = e_1.idprovincia AND ah1.idestacion::text = e_1.idestacion::text
          WHERE e_1.estado::text = 'Activa'::text AND (e_1.idprovincia = ANY (ARRAY[22, 44, 50]))
          ORDER BY ah1.fecha DESC, (
                CASE
                    WHEN ah1.horamin = 2400 THEN 0
                    ELSE ah1.horamin::integer
                END::smallint) DESC) dfc;

-- Permissions

ALTER TABLE paper.gv_aragoneseclimaticstationsdata OWNER TO postgres;
GRANT ALL ON TABLE paper.gv_aragoneseclimaticstationsdata TO postgres;


-- paper.gv_aragoneseclimaticstationsdatalastrecord source

CREATE OR REPLACE VIEW paper.gv_aragoneseclimaticstationsdatalastrecord
AS SELECT f.fecha,
    f.hora,
    f.horamin,
    f.nombrecorto,
    f.idprovincia,
    f.idestacion,
    f.nombre,
    f.tempmedia,
    f.humedadmedia,
    f.velviento,
    f.dirviento,
    f.radiacion,
    f.precipitacion,
    f.tempmediacaja,
    f.tempsuelo1,
    f.tempsuelo2,
    f.codtempsuelo2,
    f.humhoja,
    f.presion,
    f.inso,
    f.huso,
    f.xutm,
    f.yutm,
    f.latitude,
    f.longitude,
    f.altitud,
    f.timestampcalculado
   FROM ( SELECT d.fecha,
            d.hora,
            d.horamin,
            d.nombrecorto,
            d.idprovincia,
            d.idestacion,
            d.nombre,
            d.tempmedia,
            d.humedadmedia,
            d.velviento,
            d.dirviento,
            d.radiacion,
            d.precipitacion,
            d.tempmediacaja,
            d.tempsuelo1,
            d.tempsuelo2,
            d.codtempsuelo2,
            d.humhoja,
            d.presion,
            d.inso,
            d.huso,
            d.xutm,
            d.yutm,
            d.latitude,
            d.longitude,
            d.altitud,
            d.nominalsamplesnumber,
            d.timestampcalculado
           FROM paper.gv_aragoneseclimaticstationsdata d
             JOIN ( SELECT a.nombrecorto,
                    max(a.timestampcalculado) AS timestampcalculado
                   FROM paper.gv_aragoneseclimaticstationsdata a
                  WHERE a.fecha >= (CURRENT_DATE - '14 days'::interval)
                  GROUP BY a.nombrecorto) e ON d.nombrecorto::text = e.nombrecorto::text AND d.timestampcalculado = e.timestampcalculado
          WHERE d.fecha >= (CURRENT_DATE - '14 days'::interval)) f
  ORDER BY f.nombrecorto, f.fecha;

-- Permissions

ALTER TABLE paper.gv_aragoneseclimaticstationsdatalastrecord OWNER TO postgres;
GRANT ALL ON TABLE paper.gv_aragoneseclimaticstationsdatalastrecord TO postgres;

-- paper.gv_aragoneseclimaticstationsdata2consider source

CREATE OR REPLACE VIEW paper.gv_aragoneseclimaticstationsdata2consider
AS SELECT c.timestampcalculado AS lastrecord,
    wfd.fecha,
    wfd.hora,
    wfd.horamin,
    wfd.nombrecorto,
    wfd.idprovincia,
    wfd.idestacion,
    wfd.nombre,
    wfd.tempmedia,
    wfd.humedadmedia,
    wfd.velviento,
    wfd.dirviento,
    wfd.radiacion,
    wfd.precipitacion,
    wfd.tempmediacaja,
    wfd.tempsuelo1,
    wfd.tempsuelo2,
    wfd.codtempsuelo2,
    wfd.humhoja,
    wfd.presion,
    wfd.inso,
    wfd.huso,
    wfd.xutm,
    wfd.yutm,
    wfd.latitude,
    wfd.longitude,
    wfd.altitud,
    wfd.timestampcalculado
   FROM ( SELECT gv_aragoneseclimaticstationsdata.fecha,
            gv_aragoneseclimaticstationsdata.hora,
            gv_aragoneseclimaticstationsdata.horamin,
            gv_aragoneseclimaticstationsdata.nombrecorto,
            gv_aragoneseclimaticstationsdata.idprovincia,
            gv_aragoneseclimaticstationsdata.idestacion,
            gv_aragoneseclimaticstationsdata.nombre,
            gv_aragoneseclimaticstationsdata.tempmedia,
            gv_aragoneseclimaticstationsdata.humedadmedia,
            gv_aragoneseclimaticstationsdata.velviento,
            gv_aragoneseclimaticstationsdata.dirviento,
            gv_aragoneseclimaticstationsdata.radiacion,
            gv_aragoneseclimaticstationsdata.precipitacion,
            gv_aragoneseclimaticstationsdata.tempmediacaja,
            gv_aragoneseclimaticstationsdata.tempsuelo1,
            gv_aragoneseclimaticstationsdata.tempsuelo2,
            gv_aragoneseclimaticstationsdata.codtempsuelo2,
            gv_aragoneseclimaticstationsdata.humhoja,
            gv_aragoneseclimaticstationsdata.presion,
            gv_aragoneseclimaticstationsdata.inso,
            gv_aragoneseclimaticstationsdata.huso,
            gv_aragoneseclimaticstationsdata.xutm,
            gv_aragoneseclimaticstationsdata.yutm,
            gv_aragoneseclimaticstationsdata.latitude,
            gv_aragoneseclimaticstationsdata.longitude,
            gv_aragoneseclimaticstationsdata.altitud,
            gv_aragoneseclimaticstationsdata.timestampcalculado
           FROM paper.gv_aragoneseclimaticstationsdata) wfd
     JOIN paper.gv_aragoneseclimaticstationsdatalastrecord c ON wfd.idestacion = c.idestacion AND wfd.timestampcalculado < c.timestampcalculado
  ORDER BY wfd.idestacion, wfd.fecha DESC, wfd.hora DESC;

-- Permissions

ALTER TABLE paper.gv_aragoneseclimaticstationsdata2consider OWNER TO postgres;
GRANT ALL ON TABLE paper.gv_aragoneseclimaticstationsdata2consider TO postgres;

-- paper.weatherforecastprovider definition

-- Drop table

-- DROP TABLE paper.weatherforecastprovider;

CREATE TABLE paper.weatherforecastprovider (
	id bigserial NOT NULL,
	"name" varchar NOT NULL,
	active varchar DEFAULT true NOT NULL,
	CONSTRAINT weatherforecastprovider_pk PRIMARY KEY (id)
);

-- Permissions

ALTER TABLE paper.weatherforecastprovider OWNER TO postgres;
GRANT INSERT, DELETE, UPDATE, REFERENCES, TRIGGER, TRUNCATE, SELECT ON TABLE paper.weatherforecastprovider TO postgres;

-- paper.weatherforecast definition

-- Drop table

-- DROP TABLE paper.weatherforecast;

CREATE TABLE paper.weatherforecast (
	id bigserial NOT NULL,
	idestacion int8 NOT NULL,
	long numeric(10, 6) NULL,
	lat numeric(10, 6) NULL,
	forecastdate date NOT NULL,
	weatherforecastproviderid int8 NOT NULL,
	resolutionid int8 NULL,
	CONSTRAINT weatherforecast_pk PRIMARY KEY (id)
);
CREATE INDEX weatherforecast_forecastdate_idx ON paper.weatherforecast USING btree (forecastdate, idestacion);
CREATE INDEX weatherforecast_idestacion_idx ON paper.weatherforecast USING btree (idestacion, forecastdate, weatherforecastproviderid, resolutionid);

-- Permissions

ALTER TABLE paper.weatherforecast OWNER TO postgres;
GRANT INSERT, DELETE, UPDATE, REFERENCES, TRIGGER, TRUNCATE, SELECT ON TABLE paper.weatherforecast TO postgres;


-- paper.weatherforecast foreign keys

ALTER TABLE paper.weatherforecast ADD CONSTRAINT weatherforecast_fk FOREIGN KEY (idestacion) REFERENCES paper.estaragon(id);
ALTER TABLE paper.weatherforecast ADD CONSTRAINT weatherforecast_fk_1 FOREIGN KEY (weatherforecastproviderid) REFERENCES paper.weatherforecastprovider(id);

-- paper.weatherforecastdata definition

-- Drop table

-- DROP TABLE paper.weatherforecastdata;

CREATE TABLE paper.weatherforecastdata (
	id bigserial NOT NULL,
	idweatherforecast int8 NOT NULL,
	forecasttimestamp timestamp NOT NULL,
	temperature float8 NULL,
	dewpoint float8 NULL,
	relativehumidity float8 NULL,
	winddirection float8 NULL,
	windspeed float8 NULL,
	precipitation float8 NULL,
	radiation float8 NULL,
	CONSTRAINT weatherforecastdata_pk PRIMARY KEY (id)
);
CREATE INDEX weatherforecastdata_forecasttimestamp_idx ON paper.weatherforecastdata USING btree (forecasttimestamp);
CREATE INDEX weatherforecastdata_idweatherforecast_idx ON paper.weatherforecastdata USING btree (idweatherforecast, forecasttimestamp);

-- Permissions

ALTER TABLE paper.weatherforecastdata OWNER TO postgres;
GRANT INSERT, DELETE, UPDATE, REFERENCES, TRIGGER, TRUNCATE, SELECT ON TABLE paper.weatherforecastdata TO postgres;


-- paper.weatherforecastdata foreign keys

ALTER TABLE paper.weatherforecastdata ADD CONSTRAINT weatherforecastdata_fk FOREIGN KEY (idweatherforecast) REFERENCES paper.weatherforecast(id);



-- paper.gv_aragoneseclimaticstationsrecentforecastdatadatefrom source

CREATE OR REPLACE VIEW paper.gv_aragoneseclimaticstationsrecentforecastdatadatefrom
AS SELECT wfd.id,
    wfd.idweatherforecast,
    wfd.forecasttimestamp,
    wfd.temperature,
    wfd.dewpoint,
    wfd.relativehumidity,
    wfd.winddirection,
    wfd.windspeed,
    wfd.precipitation,
    wfd.radiation,
    w.weatherforecastproviderid,
    w.idestacion
   FROM ( SELECT wf.id,
            wf.idestacion,
            wf.long,
            wf.lat,
            wf.forecastdate,
            wf.weatherforecastproviderid,
            wf.resolutionid
           FROM paper.weatherforecast wf
          WHERE wf.forecastdate >= (CURRENT_DATE - '7 days'::interval)) w
     JOIN ( SELECT wfdi.id,
            wfdi.idweatherforecast,
            wfdi.forecasttimestamp,
                CASE
                    WHEN wfdi.temperature > 100::double precision THEN wfdi.temperature - 273::double precision
                    ELSE wfdi.temperature
                END AS temperature,
                CASE
                    WHEN wfdi.dewpoint > 100::double precision THEN wfdi.temperature - 273::double precision
                    ELSE wfdi.dewpoint
                END AS dewpoint,
            wfdi.relativehumidity,
            wfdi.winddirection,
            wfdi.windspeed,
            wfdi.precipitation,
            wfdi.radiation
           FROM paper.weatherforecastdata wfdi
          WHERE wfdi.forecasttimestamp >= (CURRENT_DATE - '2 days'::interval)) wfd ON w.id = wfd.idweatherforecast;

-- Permissions

ALTER TABLE paper.gv_aragoneseclimaticstationsrecentforecastdatadatefrom OWNER TO postgres;
GRANT ALL ON TABLE paper.gv_aragoneseclimaticstationsrecentforecastdatadatefrom TO postgres;

-- paper.gv_aragoneseclimaticstationsforecastdatefrom source

CREATE OR REPLACE VIEW paper.gv_aragoneseclimaticstationsforecastdatefrom
AS SELECT g.fecha AS lastdate,
    g.hora AS lasttime,
    g.idestacion,
    g.nombrecorto,
    g.huso,
    g.xutm,
    g.yutm,
    g.latitude,
    g.longitude,
    g.altitud,
    g.idprovincia,
    g.nombre,
    g.timestampcalculado AS lastdatatimestamp
   FROM paper.gv_aragoneseclimaticstationsdatalastrecord g;

-- Permissions

ALTER TABLE paper.gv_aragoneseclimaticstationsforecastdatefrom OWNER TO postgres;
GRANT ALL ON TABLE paper.gv_aragoneseclimaticstationsforecastdatefrom TO postgres;



CREATE OR REPLACE VIEW paper.gv_aragoneseclimaticstationsrecentforecastdatadatefrom
AS SELECT wfd.id,
    wfd.idweatherforecast,
    wfd.forecasttimestamp,
    wfd.temperature,
    wfd.dewpoint,
    wfd.relativehumidity,
    wfd.winddirection,
    wfd.windspeed,
    wfd.precipitation,
    wfd.radiation,
    w.weatherforecastproviderid,
    w.idestacion
   FROM ( SELECT wf.id,
            wf.idestacion,
            wf.long,
            wf.lat,
            wf.forecastdate,
            wf.weatherforecastproviderid,
            wf.resolutionid
           FROM weatherforecast wf
          WHERE wf.forecastdate >= (CURRENT_DATE - '7 days'::interval)) w
     JOIN ( SELECT wfdi.id,
            wfdi.idweatherforecast,
            wfdi.forecasttimestamp,
                CASE
                    WHEN wfdi.temperature > 100::double precision THEN wfdi.temperature - 273::double precision
                    ELSE wfdi.temperature
                END AS temperature,
                CASE
                    WHEN wfdi.dewpoint > 100::double precision THEN wfdi.temperature - 273::double precision
                    ELSE wfdi.dewpoint
                END AS dewpoint,
            wfdi.relativehumidity,
            wfdi.winddirection,
            wfdi.windspeed,
            wfdi.precipitation,
            wfdi.radiation
           FROM weatherforecastdata wfdi
          WHERE wfdi.forecasttimestamp >= (CURRENT_DATE - '2 days'::interval)) wfd ON w.id = wfd.idweatherforecast;

-- Permissions

ALTER TABLE paper.gv_aragoneseclimaticstationsrecentforecastdatadatefrom OWNER TO postgres;
GRANT ALL ON TABLE paper.gv_aragoneseclimaticstationsrecentforecastdatadatefrom TO postgres;

CREATE OR REPLACE VIEW paper.gv_aragoneseclimaticstationsrecentforecastdatadatefrom
AS SELECT wfd.id,
    wfd.idweatherforecast,
    wfd.forecasttimestamp,
    wfd.temperature,
    wfd.dewpoint,
    wfd.relativehumidity,
    wfd.winddirection,
    wfd.windspeed,
    wfd.precipitation,
    wfd.radiation,
    w.weatherforecastproviderid,
    w.idestacion
   FROM ( SELECT wf.id,
            wf.idestacion,
            wf.long,
            wf.lat,
            wf.forecastdate,
            wf.weatherforecastproviderid,
            wf.resolutionid
           FROM weatherforecast wf
          WHERE wf.forecastdate >= (CURRENT_DATE - '7 days'::interval)) w
     JOIN ( SELECT wfdi.id,
            wfdi.idweatherforecast,
            wfdi.forecasttimestamp,
                CASE
                    WHEN wfdi.temperature > 100::double precision THEN wfdi.temperature - 273::double precision
                    ELSE wfdi.temperature
                END AS temperature,
                CASE
                    WHEN wfdi.dewpoint > 100::double precision THEN wfdi.temperature - 273::double precision
                    ELSE wfdi.dewpoint
                END AS dewpoint,
            wfdi.relativehumidity,
            wfdi.winddirection,
            wfdi.windspeed,
            wfdi.precipitation,
            wfdi.radiation
           FROM weatherforecastdata wfdi
          WHERE wfdi.forecasttimestamp >= (CURRENT_DATE - '2 days'::interval)) wfd ON w.id = wfd.idweatherforecast;

-- Permissions

ALTER TABLE paper.gv_aragoneseclimaticstationsrecentforecastdatadatefrom OWNER TO postgres;
GRANT ALL ON TABLE paper.gv_aragoneseclimaticstationsrecentforecastdatadatefrom TO postgres;

-- paper.gv_aragoneseclimaticstationsforecastdatefrom source

CREATE OR REPLACE VIEW paper.gv_aragoneseclimaticstationsforecastdatefrom
AS SELECT g.fecha AS lastdate,
    g.hora AS lasttime,
    g.idestacion,
    g.nombrecorto,
    g.huso,
    g.xutm,
    g.yutm,
    g.latitude,
    g.longitude,
    g.altitud,
    g.idprovincia,
    g.nombre,
    g.timestampcalculado AS lastdatatimestamp
   FROM paper.gv_aragoneseclimaticstationsdatalastrecord g;

-- Permissions

ALTER TABLE paper.gv_aragoneseclimaticstationsforecastdatefrom OWNER TO postgres;
GRANT ALL ON TABLE paper.gv_aragoneseclimaticstationsforecastdatefrom TO postgres;

CREATE OR REPLACE VIEW paper.gv_aragoneseclimaticstationsforecastdatadatefrom
AS SELECT wfd.id,
    wfd.idweatherforecast,
    wfd.forecasttimestamp,
    wfd.temperature,
    wfd.dewpoint,
    wfd.relativehumidity,
    wfd.winddirection,
    wfd.windspeed,
    wfd.precipitation,
    wfd.radiation,
    wfd.weatherforecastproviderid,
    wfd.idestacion,
    c.lastdatatimestamp
   FROM paper.gv_aragoneseclimaticstationsrecentforecastdatadatefrom wfd
     JOIN paper.gv_aragoneseclimaticstationsforecastdatefrom c ON wfd.idestacion = c.idestacion AND wfd.forecasttimestamp >= c.lastdatatimestamp;

-- Permissions

ALTER TABLE paper.gv_aragoneseclimaticstationsforecastdatadatefrom OWNER TO postgres;
GRANT ALL ON TABLE paper.gv_aragoneseclimaticstationsforecastdatadatefrom TO postgres;


CREATE OR REPLACE VIEW paper.gv_aragoneseclimaticstationsforecastdatadatefrom2consider
AS SELECT wfd3.idestacion,
    wfd3.forecasttimestamp,
    wfd3.weatherforecastproviderid,
    wfd3.idweatherforecast,
    max(wfd3.id) AS wfdid
   FROM paper.gv_aragoneseclimaticstationsrecentforecastdatadatefrom wfd3
     JOIN ( SELECT wfd2.idestacion,
            wfd2.forecasttimestamp,
            wfd2.weatherforecastproviderid,
            max(wfd2.idweatherforecast) AS idweatherforecast
           FROM paper.gv_aragoneseclimaticstationsrecentforecastdatadatefrom wfd2
             JOIN ( SELECT w.forecasttimestamp,
                    w.idestacion,
                    min(w.weatherforecastproviderid) AS weatherforecastproviderid
                   FROM paper.gv_aragoneseclimaticstationsforecastdatadatefrom w
                  GROUP BY w.idestacion, w.forecasttimestamp) wfpf ON wfd2.idestacion = wfpf.idestacion AND wfd2.forecasttimestamp = wfpf.forecasttimestamp AND wfd2.weatherforecastproviderid = wfpf.weatherforecastproviderid
          GROUP BY wfd2.idestacion, wfd2.forecasttimestamp, wfd2.weatherforecastproviderid) wfpff ON wfd3.idestacion = wfpff.idestacion AND wfd3.forecasttimestamp = wfpff.forecasttimestamp AND wfd3.weatherforecastproviderid = wfpff.weatherforecastproviderid AND wfd3.idweatherforecast = wfpff.idweatherforecast
  GROUP BY wfd3.idestacion, wfd3.forecasttimestamp, wfd3.weatherforecastproviderid, wfd3.idweatherforecast;

-- Permissions

ALTER TABLE paper.gv_aragoneseclimaticstationsforecastdatadatefrom2consider OWNER TO postgres;
GRANT ALL ON TABLE paper.gv_aragoneseclimaticstationsforecastdatadatefrom2consider TO postgres;

CREATE OR REPLACE VIEW paper.gv_forecastdata2consider
AS SELECT c.fecha,
    c.hora,
    c.horamin,
    c.nombrecorto,
    c.idprovincia,
    c.idestacion,
    c.nombre,
    c.tempmedia,
    c.humedadmedia,
    c.velviento,
    c.dirviento,
    c.radiacion,
    c.precipitacion,
    c.tempmediacaja,
    c.tempsuelo1,
    c.tempsuelo2,
    c.codtempsuelo2,
    c.humhoja,
    c.presion,
    c.inso,
    c.huso,
    c.xutm,
    c.yutm,
    c.latitude,
    c.longitude,
    c.altitud,
    c.forecasttimestamp
   FROM ( SELECT wfdas.forecasttimestamp::date AS fecha,
            wfdas.forecasttimestamp::time without time zone AS hora,
            date_part('hour'::text, wfdas.forecasttimestamp) * 100::double precision + date_part('min'::text, wfdas.forecasttimestamp) AS horamin,
                CASE
                    WHEN wfdas.temperature >= '-100'::integer::double precision AND wfdas.temperature <= 100::double precision THEN wfdas.temperature::numeric(6,2)
                    ELSE NULL::numeric(6,2)
                END AS tempmedia,
                CASE
                    WHEN wfdas.relativehumidity >= 100::double precision THEN 100::numeric(6,2)
                    WHEN wfdas.relativehumidity < 0::double precision THEN 0::numeric(6,2)
                    ELSE wfdas.relativehumidity::numeric(6,2)
                END AS humedadmedia,
                CASE
                    WHEN wfdas.windspeed >= 1000::double precision THEN 1000::numeric(6,2)
                    WHEN wfdas.windspeed < 0::double precision THEN 0::numeric(6,2)
                    ELSE wfdas.windspeed::numeric(6,2)
                END AS velviento,
                CASE
                    WHEN wfdas.winddirection > 360::double precision THEN wfdas.winddirection::integer % 360
                    WHEN wfdas.winddirection < 0::double precision THEN wfdas.winddirection::integer % 360
                    ELSE wfdas.winddirection::integer
                END AS dirviento,
                CASE
                    WHEN wfdas.radiation >= 5000::double precision THEN wfdas.radiation::integer::numeric % 5000::numeric(6,2)
                    WHEN wfdas.radiation >= 0::double precision AND wfdas.radiation < 5000::double precision THEN wfdas.radiation::numeric(6,2)
                    ELSE 0::numeric(6,2)
                END::numeric(6,2) AS radiacion,
            wfdas.precipitation::numeric(6,2) AS precipitacion,
            NULL::numeric(6,2) AS tempmediacaja,
            NULL::numeric(6,2) AS tempsuelo1,
            NULL::numeric(6,2) AS tempsuelo2,
            NULL::numeric(6,2) AS codtempsuelo2,
            NULL::numeric(6,2) AS humhoja,
            NULL::numeric(6,2) AS presion,
            NULL::numeric(6,2) AS inso,
            wfdas.idestacion::integer AS idestacion,
            lfdfs.huso,
            lfdfs.xutm,
            lfdfs.yutm,
            lfdfs.latitude,
            lfdfs.longitude,
            lfdfs.altitud,
            lfdfs.nombrecorto,
            lfdfs.nombre,
            lfdfs.idprovincia,
            wfdas.forecasttimestamp
           FROM paper.gv_aragoneseclimaticstationsrecentforecastdatadatefrom wfdas
             JOIN ( SELECT a.idestacion,
                    a.forecasttimestamp,
                    a.weatherforecastproviderid,
                    a.idweatherforecast,
                    a.wfdid,
                    b.huso,
                    b.utmx AS xutm,
                    b.utmy AS yutm,
                    b.lat AS latitude,
                    b.long AS longitude,
                    b.alt AS altitud,
                    b.nombrecorto,
                    b.nombre,
                    b.idprovincia
                   FROM paper.gv_aragoneseclimaticstationsforecastdatadatefrom2consider a
                     JOIN paper.gv_estaragonrestringido b ON a.idestacion = b.idestacion::bigint) lfdfs ON wfdas.id = lfdfs.wfdid) c;

-- Permissions

ALTER TABLE paper.gv_forecastdata2consider OWNER TO postgres;
GRANT ALL ON TABLE paper.gv_forecastdata2consider TO postgres;

-- paper.gv_forecastdata2considerstations source


-- paper.gv_estaragon24 source

CREATE OR REPLACE VIEW paper.gv_estaragon24
AS SELECT ea.id,
    ea.geom,
    ea.indicativo,
    ea.nombre,
    ea.provincia,
    ea.altitud,
    ea.datum,
    ea.idestacion,
    ea.ubi,
    ea.idprovincia,
    ea.huso,
    ea.idtermino,
    ea.nombrecorto,
    ea.estado,
    ea.layer,
    ea.utmx,
    ea.utmy,
    ea.alt,
    ea.long,
    ea.lat,
    ea.considered,
    ea.location,
    ea.pocid,
    ea.nominalsamplesnumber
   FROM paper.gv_estaragon ea
  WHERE ea.nominalsamplesnumber = 24 AND ea.considered;

-- Permissions

ALTER TABLE paper.gv_estaragon24 OWNER TO postgres;
GRANT ALL ON TABLE paper.gv_estaragon24 TO postgres;



CREATE OR REPLACE VIEW paper.gv_aragoneseclimaticstationsdatawithforecastandestimatedata
AS SELECT dwf.fecha,
    dwf.hora,
    dwf.horamin,
    dwf.nombrecorto::text AS nombrecorto,
    dwf.idprovincia,
    dwf.idestacion,
    dwf.nombre,
    dwf.tempmedia,
    dwf.humedadmedia,
    dwf.velviento,
    dwf.dirviento,
    dwf.radiacion,
    dwf.precipitacion,
    dwf.tempmediacaja,
    dwf.tempsuelo1,
    dwf.tempsuelo2,
    dwf.codtempsuelo2,
    dwf.humhoja,
    dwf.presion,
    dwf.inso,
    dwf.huso,
    dwf.xutm,
    dwf.yutm,
    dwf.latitude,
    dwf.longitude,
    dwf.altitud,
    dwf.datatimestamp
   FROM ( SELECT cd.fecha,
            cd.hora,
            cd.horamin,
            cd.nombrecorto,
            cd.idprovincia,
            cd.idestacion,
            cd.nombre,
            cd.tempmedia,
            cd.humedadmedia,
            cd.velviento,
            cd.dirviento,
            cd.radiacion,
            cd.precipitacion,
            cd.tempmediacaja,
            cd.tempsuelo1,
            cd.tempsuelo2,
            cd.codtempsuelo2,
            cd.humhoja,
            cd.presion,
            cd.inso,
            cd.huso,
            cd.xutm,
            cd.yutm,
            cd.latitude,
            cd.longitude,
            cd.altitud,
            cd.timestampcalculado AS datatimestamp
           FROM paper.gv_aragoneseclimaticstationsdata2consider cd
        UNION ALL
         SELECT gv_forecastdata2considerstations.fecha,
            gv_forecastdata2considerstations.hora,
            gv_forecastdata2considerstations.horamin,
                CASE
                    WHEN gv_forecastdata2considerstations.nombrecorto::text ~~ 'GV%'::text THEN lower(gv_forecastdata2considerstations.nombrecorto::text)::character varying
                    ELSE gv_forecastdata2considerstations.nombrecorto
                END AS nombrecorto,
            gv_forecastdata2considerstations.idprovincia,
            gv_forecastdata2considerstations.idestacion,
            gv_forecastdata2considerstations.nombre,
            gv_forecastdata2considerstations.tempmedia,
            gv_forecastdata2considerstations.humedadmedia,
            gv_forecastdata2considerstations.velviento,
            gv_forecastdata2considerstations.dirviento,
            gv_forecastdata2considerstations.radiacion,
            gv_forecastdata2considerstations.precipitacion,
            NULL::text AS tempmediacaja,
            gv_forecastdata2considerstations.tempsuelo1,
            NULL::text AS tempsuelo2,
            NULL::text AS codtempsuelo2,
            gv_forecastdata2considerstations.humhoja,
            gv_forecastdata2considerstations.presion,
            gv_forecastdata2considerstations.inso,
            gv_forecastdata2considerstations.huso,
            gv_forecastdata2considerstations.xutm,
            gv_forecastdata2considerstations.yutm,
            gv_forecastdata2considerstations.latitude,
            gv_forecastdata2considerstations.longitude,
            gv_forecastdata2considerstations.altitud,
            gv_forecastdata2considerstations.forecasttimestamp AS datatimestamp
           FROM paper.gv_forecastdata2considerstations
        UNION ALL
         SELECT a.fecha,
            a.hora,
            a.horamin,
            a.nombrecorto,
            a.idprovincia,
            a.idestacion,
            a.nombre,
            a.tempmedia,
            a.humedadmedia,
            a.velviento,
            a.dirviento,
            a.radiacion,
            a.precipitacion,
            a.tempmediacaja,
            a.tempsuelo1,
            a.tempsuelo2::text AS tempsuelo2,
            a.codtempsuelo2,
            a.humhoja,
            a.presion,
            a.inso,
            a.huso,
            a.xutm,
            a.yutm,
            a.latitude,
            a.longitude,
            a.altitude,
            a.timestampcalculado AS datatimestamp
           FROM paper.gv_datosestacionesreemplazarhormin a) dwf;

-- Permissions

ALTER TABLE paper.gv_aragoneseclimaticstationsdatawithforecastandestimatedata OWNER TO postgres;
GRANT ALL ON TABLE paper.gv_aragoneseclimaticstationsdatawithforecastandestimatedata TO postgres;



CREATE OR REPLACE VIEW paper.gv_forecastdata2considerstations
AS SELECT fdcs_total.fecha,
    fdcs_total.hora,
    fdcs_total.horamin,
        CASE
            WHEN fdcs_total.nombrecorto::text ~~ 'GV%'::text THEN lower(fdcs_total.nombrecorto::text)::character varying
            ELSE fdcs_total.nombrecorto
        END AS nombrecorto,
    fdcs_total.idprovincia,
    fdcs_total.idestacion,
    fdcs_total.nombre,
    fdcs_total.tempmedia,
    fdcs_total.humedadmedia,
    fdcs_total.velviento,
    fdcs_total.dirviento,
    fdcs_total.radiacion,
    fdcs_total.precipitacion,
    fdcs_total.tempmediacaja,
    fdcs_total.tempsuelo1,
    fdcs_total.tempsuelo2,
    fdcs_total.codtempsuelo2,
    fdcs_total.humhoja,
    fdcs_total.presion,
    fdcs_total.inso,
    fdcs_total.huso,
    fdcs_total.xutm,
    fdcs_total.yutm,
    fdcs_total.latitude,
    fdcs_total.longitude,
    fdcs_total.altitud,
    fdcs_total.forecasttimestamp
   FROM ( SELECT wfd.fecha,
            wfd.hora,
            wfd.horamin,
            wfd.nombrecorto,
            wfd.idprovincia,
            wfd.idestacion,
            wfd.nombre,
            wfd.tempmedia,
            wfd.humedadmedia,
            wfd.velviento,
            wfd.dirviento,
            wfd.radiacion,
            wfd.precipitacion,
            wfd.tempmediacaja,
            wfd.tempsuelo1,
            wfd.tempsuelo2,
            wfd.codtempsuelo2,
            wfd.humhoja,
            wfd.presion,
            wfd.inso,
            wfd.huso,
            wfd.xutm,
            wfd.yutm,
            wfd.latitude,
            wfd.longitude,
            wfd.altitud,
            wfd.forecasttimestamp
           FROM paper.gv_forecastdata2consider wfd
             JOIN paper.gv_estaragon24 ea24 ON upper(wfd.nombrecorto::text) = upper(ea24.nombrecorto::text)
        UNION ALL
         SELECT wfd.fecha,
            wfd.hora,
            wfd.horamin,
            wfd.nombrecorto,
            wfd.idprovincia,
            wfd.idestacion,
            wfd.nombre,
            wfd.tempmedia,
            wfd.humedadmedia,
            wfd.velviento,
            wfd.dirviento,
            wfd.radiacion / 2::numeric AS radiacion,
            wfd.precipitacion / 2::numeric AS precipitacion,
            wfd.tempmediacaja,
            wfd.tempsuelo1,
            wfd.tempsuelo2,
            wfd.codtempsuelo2,
            wfd.humhoja,
            wfd.presion,
            wfd.inso / 2::numeric AS inso,
            wfd.huso,
            wfd.xutm,
            wfd.yutm,
            wfd.latitude,
            wfd.longitude,
            wfd.altitud,
            wfd.forecasttimestamp
           FROM paper.gv_forecastdata2consider wfd
             JOIN paper.gv_estaragon48 ea48 ON upper(wfd.nombrecorto::text) = upper(ea48.nombrecorto::text)
        UNION ALL
         SELECT wfd.fecha,
            wfd.hora + 30::double precision * '00:01:00'::interval AS hora,
            wfd.horamin + 30::double precision,
            wfd.nombrecorto,
            wfd.idprovincia,
            wfd.idestacion,
            wfd.nombre,
            wfd.tempmedia,
            wfd.humedadmedia,
            wfd.velviento,
            wfd.dirviento,
            wfd.radiacion / 2::numeric AS radiacion,
            wfd.precipitacion / 2::numeric AS precipitacion,
            wfd.tempmediacaja,
            wfd.tempsuelo1,
            wfd.tempsuelo2,
            wfd.codtempsuelo2,
            wfd.humhoja,
            wfd.presion,
            wfd.inso / 2::numeric AS inso,
            wfd.huso,
            wfd.xutm,
            wfd.yutm,
            wfd.latitude,
            wfd.longitude,
            wfd.altitud,
            wfd.forecasttimestamp + ((30 || 'minutes'::text)::interval)
           FROM paper.gv_forecastdata2consider wfd
             JOIN paper.gv_estaragon48 ea48 ON upper(wfd.nombrecorto::text) = upper(ea48.nombrecorto::text)) fdcs_total;

-- Permissions

ALTER TABLE paper.gv_forecastdata2considerstations OWNER TO postgres;
GRANT ALL ON TABLE paper.gv_forecastdata2considerstations TO postgres;
