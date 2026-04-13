# Keeping up with games

Using data to stay up-to-date on video games!

## Business case

I haven't played video games for a while and I am out of touch with what games are considered cool and which upcoming releases are highly anticipated. Let's resolve this using the Internet Game DataBase (IGDB) data from Twitch!

To do so, the end goal should be to:
* Find insights into **what games are currently popular**
* Have an overview of upcoming releases

Explicit tasks:
* Build a datamodel that can be leveraged for BI.
* Create a dashboard that provides insights that higlights features of successful games.


## Project methodology and reasoning

### Architecture
![Architecture](diagrams\Architecture.png)

#### Tech Stack Rationale

* An ELT structure was adopted to preserve source data integrity, while providing a clear and logical progression through the Bronze, Silver, and Gold layers.
* Python, Snowflake, and Power BI were selected as the core stack based on existing 
proficiency, maximising both delivery speed and output quality.
* dbt was used to handle the transformation layer within the ELT pipeline.
* Utilizing Airflow to orchestrate the whole pipeline.
* Source data is coming from the IGDB API, referencing multiple of it's endpoints.
* Key-pair authentication is configured for the Snowflake service user to ensure secure access.


