# Baseball Card Collection Database

Baseball card and player data for years 1970 - 1989


### Project Description

This project is a MSSQL database designed to manage a baseball card collection. It includes tables that reference player stats, teams, positions, and other related information. The database can be used to track and analyze a user's baseball card collection, as well as provide insights into player and team performance based on historical data. The project is designed to be scalable and customizable, allowing users to add additional tables or modify existing ones to suit their specific needs. The goal of the project is to provide an easy-to-use, comprehensive tool for managing and analyzing baseball card collections.

### Usage

1. Open SQL Server Management Studio and connect to the MSSQL server where you created the Baseball Card Collection database.
2. Once connected, open a new query window and execute the SQL script provided with the project to create the necessary tables and relationships.
3. Import your baseball card collection data into the database. This can be done using the Import/Export Wizard in SQL Server Management Studio or by executing SQL INSERT statements for each record.
4. Once your data is imported, you can query the database to retrieve player stats, team information, and other related data. For example, you might use the following SQL statement to retrieve batting statistics for a specific player:

SELECT * 
FROM players_attributes as patt
  JOIN players_stats_by_year pstats ON players_attributes.player_id = players_stats_by_year.player_id
WHERE patt.player_firstname = 'George'
  AND patt.player_lastname = 'Foster'
  
5. You can also use SQL queries to perform more complex analysis on your data, such as calculating batting averages, on-base percentages, and other statistical measures.
6. To modify the database schema, add additional tables, or make other changes, open the SQL script provided with the project and modify it as needed. Then, execute the modified script to apply your changes to the database.
7.Remember to back up your database regularly to prevent data loss in case of system failure or other issues.tion.]

### Contributing

We welcome contributions from the community and are grateful for any feedback on this project. To contribute, please follow these steps:

1. Fork the project repository and clone it to your local machine.
2. Create a new branch for your changes and switch to it.
3. Make your changes, test them thoroughly, and ensure that your code follows the project's coding standards.
4. Commit your changes and push your branch to your forked repository.
5. Open a pull request against the main branch of the original repository and describe your changes and the problem they solve.
6. Our team will review your changes and provide feedback as necessary.

Please note that we have a code of conduct for all contributors to this project. By participating in this project, you agree to abide by its terms. 

### License

Please refer to LICENSE.sql



