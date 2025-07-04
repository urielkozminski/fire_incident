Analytics Queries explanation:


1. daily_incident_count_by_weekday

Explanation:
    This query calculates the total number of fire incidents aggregated by each day of the week (Sunday=1, Saturday=7).

Insight:
    Understanding incident patterns by weekday helps identify if there are specific days with consistently higher or lower fire activity. This can reflect social behavior patterns, staffing needs, or risk factors.

Impact:

    Fire departments can optimize resource allocation and scheduling based on peak days.

    Public safety campaigns can be timed to target high-risk days.

    Analysts can investigate causes for weekday variation.


2. response_time_distribution

Explanation:
    This query groups days into buckets based on the average fire response time in seconds and counts how many days fall into each bucket.

Insight:
    Response time is a critical metric for emergency services, impacting outcomes and safety. Categorizing days by response time buckets highlights how often the department meets response targets or experiences delays.

Impact:

    Identify if most days have quick response times or if delays are common.

    Monitor the impact of interventions aiming to reduce response times.

    Communicate performance to stakeholders with easy-to-understand categories.


3. top_5_neighborhood_per_month_by_incident_count:

Explanation:
    This query ranks neighborhoods monthly by their number of fire incidents and returns the top 5 neighborhoods with the highest incident counts for each month.

Insight:
    Identifying neighborhoods with frequent incidents helps focus prevention efforts, allocate firefighting resources, and engage local communities.

Impact:

    Prioritize neighborhoods for fire safety inspections or outreach programs.

    Track whether interventions reduce incidents in high-risk areas.

    Support decision-making for deployment of firefighting personnel.


4. top_10_cities_with_most_fatalities:

Explanation:
    This query retrieves the top 10 cities with the highest total number of fire-related fatalities and the number of incidents involving fatalities.

Insight:
    Fatalities are the most severe outcome of fire incidents. Highlighting cities with high fatality counts signals where urgent attention is required.

Impact:

    Direct fire prevention and education programs to cities with high fatality rates.

    Inform policy and funding decisions for emergency response improvements.

    Track progress over time in reducing fatal fire incidents.


5. trend_incidents_yoy:

Explanation:
    This query counts total fire incidents per year, showing the year-over-year trend.

Insight:
    Analyzing incident trends over multiple years reveals whether fire incidents are increasing, stable, or declining.

Impact:

    Evaluate effectiveness of fire prevention policies over time.

    Forecast future resource needs based on incident trends.

    Identify external factors or changes influencing fire incident frequency.

