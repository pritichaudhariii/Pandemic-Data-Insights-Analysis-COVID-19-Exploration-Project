-- 1) Case fatality rate (CFR) over time in Canada (weekly)
SELECT
  Location,
  `date`,
  total_cases,
  total_deaths,
  (CAST(total_deaths AS DECIMAL(20,6)) / NULLIF(CAST(total_cases AS DECIMAL(20,6)), 0)) * 100 AS infection_death_percentage
FROM covidproject.coviddeaths
WHERE Location = 'Canada'
  AND MOD(DATEDIFF(`date`, '2020-01-05'), 7) = 0
ORDER BY Location, `date`;

-- 2) Infection rate vs population in Canada (weekly)
SELECT
  Location,
  `date`,
  population,
  total_cases,
  (CAST(total_cases AS DECIMAL(20,6)) / NULLIF(CAST(population AS DECIMAL(20,6)), 0)) * 100 AS infection_percentage
FROM covidproject.coviddeaths
WHERE Location = 'Canada'
  AND MOD(DATEDIFF(`date`, '2020-01-05'), 7) = 0
ORDER BY Location, `date`;

-- 3) Countries with highest total infection counts (latest max)
SELECT
  Location,
  Population,
  MAX(CAST(total_cases AS UNSIGNED)) AS max_infection_count
FROM covidproject.coviddeaths
WHERE Continent IS NOT NULL AND Continent <> ''
GROUP BY Location, Population
ORDER BY max_infection_count DESC;

-- 4) Countries with highest % infected vs population (peak)
SELECT
  Location,
  Population,
  MAX(CAST(total_cases AS UNSIGNED)) AS max_infection_count,
  MAX(CAST(total_cases AS DECIMAL(20,6)) / NULLIF(CAST(population AS DECIMAL(20,6)), 0)) * 100 AS percentage_population_infected
FROM covidproject.coviddeaths
WHERE Continent IS NOT NULL AND Continent <> ''
GROUP BY Location, Population
ORDER BY percentage_population_infected DESC;

-- 5) Countries with highest total deaths
SELECT
  Location,
  MAX(CAST(total_deaths AS UNSIGNED)) AS max_death_count
FROM covidproject.coviddeaths
WHERE Continent IS NOT NULL AND Continent <> ''
GROUP BY Location
ORDER BY max_death_count DESC;

-- 6) Countries with highest death-per-case % (using peaks)
SELECT
  Location,
  Population,
  MAX(CAST(total_cases  AS UNSIGNED)) AS total_infection_count,
  MAX(CAST(total_deaths AS UNSIGNED)) AS total_death_count,
  (MAX(CAST(total_deaths AS DECIMAL(20,6))) / NULLIF(MAX(CAST(total_cases AS DECIMAL(20,6))), 0)) * 100 AS total_death_per_case_percent
FROM covidproject.coviddeaths
WHERE Continent IS NOT NULL AND Continent <> ''
GROUP BY Location, Population
ORDER BY total_death_per_case_percent DESC;

-- 7) Max death count for continent-like aggregates (exclude income groups, World, EU)
SELECT
  Location,
  MAX(CAST(total_deaths AS UNSIGNED)) AS max_death_count
FROM covidproject.coviddeaths
WHERE (Continent IS NULL OR Continent = '')
  AND Location NOT LIKE '%income'
  AND Location NOT IN ('World', 'European Union')
GROUP BY Location
ORDER BY max_death_count DESC;

-- 8) Global weekly totals and deaths-per-case %
SELECT
  `date`,
  SUM(CAST(new_cases  AS DECIMAL(20,6))) AS daily_total_cases,
  SUM(CAST(new_deaths AS DECIMAL(20,6))) AS daily_total_deaths,
  (SUM(CAST(new_deaths AS DECIMAL(20,6))) / NULLIF(SUM(CAST(new_cases AS DECIMAL(20,6))), 0)) * 100 AS daily_death_percentage
FROM covidproject.coviddeaths
WHERE Continent IS NOT NULL AND Continent <> ''
  AND MOD(DATEDIFF(`date`, '2020-01-05'), 7) = 0
GROUP BY `date`
ORDER BY `date`;

-- 9) Global totals across the full period
SELECT
  SUM(CAST(new_cases  AS DECIMAL(20,6))) AS total_cases,
  SUM(CAST(new_deaths AS DECIMAL(20,6))) AS total_deaths,
  (SUM(CAST(new_deaths AS DECIMAL(20,6))) / NULLIF(SUM(CAST(new_cases AS DECIMAL(20,6))), 0)) * 100 AS death_percentage
FROM covidproject.coviddeaths
WHERE Continent IS NOT NULL AND Continent <> '';

-- 10) Vaccinations join + rolling people vaccinated
SELECT
  dea.continent,
  dea.location,
  dea.`date`,
  dea.population,
  vac.new_vaccinations,
  SUM(CAST(COALESCE(NULLIF(vac.new_vaccinations, ''), '0') AS UNSIGNED))
    OVER (PARTITION BY dea.location ORDER BY dea.location, dea.`date`) AS rolling_people_vaccinated
FROM covidproject.coviddeaths dea
JOIN covidproject.covidvaccinations vac
  ON dea.location = vac.location
 AND dea.`date`   = vac.`date`
WHERE dea.continent IS NOT NULL AND dea.continent <> ''
ORDER BY dea.location, dea.`date`;

-- 11) Same calc using a CTE + percent of population
WITH PopVsVac AS (
  SELECT
    dea.continent,
    dea.location,
    dea.`date`,
    dea.population,
    CAST(COALESCE(NULLIF(vac.new_vaccinations, ''), '0') AS UNSIGNED) AS new_vaccinations,
    SUM(CAST(COALESCE(NULLIF(vac.new_vaccinations, ''), '0') AS UNSIGNED))
      OVER (PARTITION BY dea.location ORDER BY dea.location, dea.`date`) AS rolling_people_vaccinated
  FROM covidproject.coviddeaths dea
  JOIN covidproject.covidvaccinations vac
    ON dea.location = vac.location
   AND dea.`date`   = vac.`date`
  WHERE dea.continent IS NOT NULL AND dea.continent <> ''
)
SELECT
  *,
  (CAST(rolling_people_vaccinated AS DECIMAL(20,6)) / NULLIF(CAST(population AS DECIMAL(20,6)), 0)) * 100 AS percent_population_vaccinated
FROM PopVsVac;

-- 12) Temp table version (renamed to avoid clashing with the view)
DROP TEMPORARY TABLE IF EXISTS tmp_percent_population_vaccinated;
CREATE TEMPORARY TABLE tmp_percent_population_vaccinated (
  continent VARCHAR(255),
  location  VARCHAR(255),
  `date`    DATETIME,
  population BIGINT,
  new_vaccinations BIGINT,
  rolling_people_vaccinated BIGINT
);

INSERT INTO tmp_percent_population_vaccinated
SELECT
  dea.continent,
  dea.location,
  dea.`date`,
  dea.population,
  CAST(COALESCE(NULLIF(vac.new_vaccinations, ''), '0') AS UNSIGNED) AS new_vaccinations,
  SUM(CAST(COALESCE(NULLIF(vac.new_vaccinations, ''), '0') AS UNSIGNED))
    OVER (PARTITION BY dea.location ORDER BY dea.location, dea.`date`) AS rolling_people_vaccinated
FROM covidproject.coviddeaths dea
JOIN covidproject.covidvaccinations vac
  ON dea.location = vac.location
 AND dea.`date`   = vac.`date`
WHERE dea.continent IS NOT NULL AND dea.continent <> '';

SELECT
  *,
  (CAST(rolling_people_vaccinated AS DECIMAL(20,6)) / NULLIF(CAST(population AS DECIMAL(20,6)), 0)) * 100 AS percent_population_vaccinated
FROM tmp_percent_population_vaccinated;

-- 13) View for visualization (distinct, idempotent name)
CREATE OR REPLACE VIEW covidproject.vw_percent_population_vaccinated AS
SELECT
  dea.continent,
  dea.location,
  dea.`date`,
  dea.population,
  CAST(COALESCE(NULLIF(vac.new_vaccinations, ''), '0') AS UNSIGNED) AS new_vaccinations,
  SUM(CAST(COALESCE(NULLIF(vac.new_vaccinations, ''), '0') AS UNSIGNED))
    OVER (PARTITION BY dea.location ORDER BY dea.location, dea.`date`) AS rolling_people_vaccinated
FROM covidproject.coviddeaths dea
JOIN covidproject.covidvaccinations vac
  ON dea.location = vac.location
 AND dea.`date`   = vac.`date`
WHERE dea.continent IS NOT NULL AND dea.continent <> '';

-- Quick check
SELECT * FROM covidproject.vw_percent_population_vaccinated LIMIT 100;

-- Optional performance hints (run once):
-- CREATE INDEX idx_coviddeaths_location_date ON covidproject.coviddeaths (location, `date`);
-- CREATE INDEX idx_coviddeaths_continent     ON covidproject.coviddeaths (continent);
-- CREATE INDEX idx_vaccinations_location_date ON covidproject.covidvaccinations (location, `date`);
