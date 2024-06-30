Select *
from PortfolioProject1..CovidDeaths
where continent is not null
order by 3,4

--Select *
--from PortfolioProject1..CovidVaccinations
--order by 3,4

--Select the data that we are going to be using

Select location,date,total_cases,new_cases,total_deaths,population
from PortfolioProject1..CovidDeaths
where continent is not null
order by 1,2

--total cases vs total deaths
--likelyhood of dying if you get covid in Germany
Select location,date,total_cases,total_deaths,(total_deaths/total_cases)*100 as DeathPercentage
from PortfolioProject1..CovidDeaths
Where location like '%germany%'
order by 1,2

--total cases vs population
Select location,date,population,total_cases,(total_cases/population)*100 as CasePercentage
from PortfolioProject1..CovidDeaths
Where location like '%germany%'
order by 1,2

--Looking at countries with the highest infection rate compared to population
Select location,population,MAX(total_cases) as HighestInfectionCount,max((total_cases/population))*100 as PercentagePopulationInfected
from PortfolioProject1..CovidDeaths
--Where location like '%germany%'
where continent is not null
group by population,location
order by PercentagePopulationInfected desc

--Showing countries with highest death count per population

Select location,max(cast(total_deaths as int)) as TotalDeathCount
from PortfolioProject1..CovidDeaths
--Where location like '%germany%'
where continent is not null
group by population,location
order by TotalDeathCount desc

--Try this for continents


--Showing continents with the highest death count per population

Select continent,max(cast(total_deaths as int)) as TotalDeathCount
from PortfolioProject1..CovidDeaths
--Where location like '%germany%'
where continent is not null
group by continent
order by TotalDeathCount desc


--GLOBAL NUMBERS

Select SUM(new_cases) as Total_cases, sum(cast(new_deaths as int)) as Total_deaths,sum(cast(new_deaths as int))/SUM(new_cases)*100 as DeathPercentage
from PortfolioProject1..CovidDeaths
--Where location like '%germany%'
Where continent IS NOT NULL 
--GROUP BY date
order by 1,2


-- Looking at total population vs vaccinations
--CTE

WITH PopvsVac (Continent,Location,Date,Population,New_Vaccinations,RollingPeopleVaccinated)
as
(
Select dea.continent,dea.location,dea.date, dea.population, vac.new_vaccinations
,SUM(cast(vac.new_vaccinations as int)) over (Partition by dea.location order by dea.location,dea.date) as RollingPeopleVaccinated
from PortfolioProject1..CovidDeaths dea
join PortfolioProject1..CovidVaccinations vac
	on dea.location=vac.location
	and dea.date=vac.date
where dea.continent is not null
--order by 2,3
)
select *, (RollingPeopleVaccinated/Population)*100 as PercentOfPeopleVaccinated
from PopvsVac


--TEMP TABLE
DROP Table if exists #PercentPopulationVaccinated

Create Table #PercentPopulationVaccinated
(
Continent nvarchar(255),
Location nvarchar(255),
Date datetime,
Population numeric,
New_vaccinations numeric,
RollingPeopleVaccinated numeric
)

insert into #PercentPopulationVaccinated

Select dea.continent,dea.location,dea.date, dea.population, vac.new_vaccinations
,SUM(cast(vac.new_vaccinations as int)) over (Partition by dea.location order by dea.location,dea.date) as RollingPeopleVaccinated
from PortfolioProject1..CovidDeaths dea
join PortfolioProject1..CovidVaccinations vac
	on dea.location=vac.location
	and dea.date=vac.date
where dea.continent is not null
--order by 2,3
select *, (RollingPeopleVaccinated/Population)*100 as PercentOfPeopleVaccinated
from #PercentPopulationVaccinated


--View to store data for later

Create View PercentPopulationVaccinated as
Select dea.continent,dea.location,dea.date, dea.population, vac.new_vaccinations
,SUM(cast(vac.new_vaccinations as int)) over (Partition by dea.location order by dea.location,dea.date) as RollingPeopleVaccinated
from PortfolioProject1..CovidDeaths dea
join PortfolioProject1..CovidVaccinations vac
	on dea.location=vac.location
	and dea.date=vac.date
where dea.continent is not null
--order by 2,3

Select * 
from PercentPopulationVaccinated

Create View InfectionCount as
Select location,population,MAX(total_cases) as HighestInfectionCount,max((total_cases/population))*100 as PercentagePopulationInfected
from PortfolioProject1..CovidDeaths
--Where location like '%germany%'
where continent is not null
group by population,location
--order by PercentagePopulationInfected desc

Select * 
from PercentPopulationVaccinated