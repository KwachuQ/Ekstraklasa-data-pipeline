1. Liczba match_id w full_matches_data i full_stats_data jest różna (4845 vs 3437). 
Brakuje statystyk dla późniejszych sezonów.

2. W tabeli full_matches_data znajdują się match_id dotyczące meczów, które były odwołane i przełożone - to także może mieć wpływ na tą różnicę.

3. Podstawową jednostką analizy będzie mecz (match_id). Tabele połączone po match_id.

4. W full_stats_data znajduje się 172247 rekordów, ale 3437 unikalnych match_id. Każdy mecz ma statystyki przypisane do różnych połów meczu i grup statystyk.

5. Total matches = 4845, matches_without_stats = 1408, coverage = 70.94%

6. Sezony od 08/09 do 12/13 nie mają w ogóle meczów z pokryciem statystyk. Kolejne sezony mają po kilka/kilkanaście meczów z brakującymi statystykami. Tylko sezony 17/18 i 18/19 mają pełne pokrycie.

7. Przełożone mecze - 103 (1 ze statystykami) - do usunięcia (walkower za zachowanie kibiców)
Zakończone ale status removed - 90 (7 ze statystykami) - do zachowania
Odwołane - 20 (0 ze statystykami) = do usunięcia
Zakończone ale status retired - 1 (0 ze statystykami) = do usunięcia

8. W tabeli full_matches_data NULL w kluczowych kolumnach home_score_current i away_score_current dotyczą głównie meczów odwołanych i przełożonych (potwierdzenie, że te mecze mogą być usunięte). Pojedynczy mecz ze statusem finished też moze byc usunięty.

9. Jest 7 typów statystyk z niejednolitą liczbą unikalnych meczów dla każdego typu. Oznacza to, że różne mecze będą miały dostępne różne statystyki. 

group_name	Match overview
stat_types_count	12
matches_count	3437
total_records	52898

group_name	Defending
stat_types_count	7
matches_count	1236
total_records	17062

group_name	Shots
stat_types_count	7
matches_count	3426
total_records	32227

group_name	Goalkeeping
stat_types_count	6
matches_count	3423
total_records	12536

group_name	Attack
stat_types_count	6
matches_count	3286
total_records	17869

group_name	Passes
stat_types_count	6
matches_count	3424
total_records	21115

group_name	Duels
stat_types_count	5
matches_count	1236
total_records	18540

10. Statystyki mają tylko wartości numeryczne.

11. Podział na połowy i cały mecze w statystykach:

period	ALL
matches_count	3437
records_count	81274
unique_stats	46

period	1ST
matches_count	1236
records_count	45485
unique_stats	36

period	2ND
matches_count	1236
records_count	45488
unique_stats	36

12. Brak duplikatów

13. Brak outliers w wynikach meczów.

14. Mecze przełożone lub odwołane dotyczą wielu sezonów, więc prawdopodobne jest, że te dane odpowiadają rzeczywistości, a nie że wynikają z błędów w bazie danych API.

15. groups_count	7
matches_count	1236
percentage	35.96

groups_count	5
matches_count	2049
percentage	59.62

groups_count	4
matches_count	138
percentage	4.02

groups_count	3
matches_count	2
percentage	0.06

groups_count	2
matches_count	1
percentage	0.03

groups_count	1
matches_count	11
percentage	0.32

16. coverage_type	Only ALL period
matches_count	2201
percentage	64.04

coverage_type	Complete (ALL + 1ST + 2ND)
matches_count	1236
percentage	35.96

17. tournament_name	Ekstraklasa, Relegation Round
country_name	Poland
total_matches	203
matches_with_stats	195
coverage_pct	96.06
first_season	Ekstraklasa 13/14
last_season	Ekstraklasa 19/20

tournament_name	Ekstraklasa, Championship Round
country_name	Poland
total_matches	203
matches_with_stats	193
coverage_pct	95.07
first_season	Ekstraklasa 13/14
last_season	Ekstraklasa 19/20

tournament_name	Ekstraklasa
country_name	Poland
total_matches	4350
matches_with_stats	3048
coverage_pct	70.07
first_season	Ekstraklasa 08/09
last_season	Ekstraklasa 25/26

18. Statystyki pojawiają się w 2013 roku (96 meczów z 288 ma statystyki.)

19. 
Corner kicks	Match overview	positive	event	3434	99.91
Fouls	Match overview	negative	event	3429	99.77
Free kicks	Match overview	positive	event	3425	99.65
Shots on target	Shots	positive	event	3425	99.65
Throw-ins	Passes	positive	event	3424	99.62
Shots off target	Shots	negative	event	3423	99.59
Goal kicks	Goalkeeping	positive	event	3423	99.59
Goalkeeper saves	Match overview	positive	event	3413	99.30
Total saves	Goalkeeping	positive	event	3413	99.30
Ball possession	Match overview	positive	event	3377	98.25
Yellow cards	Match overview	negative	event	3360	97.76

20.
season_name	Ekstraklasa 25/26
complete_matches	65
min_stats	38
max_stats	44
avg_stats	41

eason_name	Ekstraklasa 24/25
complete_matches	295
min_stats	35
max_stats	45
avg_stats	41

season_name	Ekstraklasa 23/24
complete_matches	289
min_stats	33
max_stats	44
avg_stats	37

season_name	Ekstraklasa 22/23
complete_matches	293
min_stats	32
max_stats	38
avg_stats	35

season_name	Ekstraklasa 21/22
complete_matches	294
min_stats	31
max_stats	36
avg_stats	34

21.

source	Matches only
tournaments	4
seasons	18

source	Stats only
tournaments	1
seasons	13

source	Common
tournaments	0
seasons	0

22. tournament_id i season_id w full_stats_data nie są NULL. Mają inne wartości, niż te w full_matches_data.

23. Postponed: 103 (1 ze statystykami) - usunąć
Removed:    90 (7 ze statystykami) - zachować 7 spotkań 
Cancelled:  20 (0 ze statystykami) - usunąć
Retired:     1 (0 ze statystykami) - usunąć

24.

data_tier	premium
matches_count	1236
avg_completeness	92.58
earliest_match	2021-07-31
latest_match	2025-09-15
seasons_count	5

data_tier	standard
matches_count	2042
avg_completeness	46.77
earliest_match	2013-09-15
latest_match	2021-05-16
seasons_count	8

data_tier	basic
matches_count	1450
avg_completeness	3.97
earliest_match	2008-08-08
latest_match	2024-03-12
seasons_count	15

data_tier	excluded
matches_count	117
avg_completeness	3.10
earliest_match	2008-07-25
latest_match	2025-09-13
seasons_count	16

25. Po analizie meczów ze statusem removed i statystykami stwierdzam, że mecze te zostaną zachowane.