# Publishing this project to GitHub

The prepared branch name is `feature/walmart-data-engineering-final`. The package is not connected to a remote GitHub repository.

1. Extract the project folder locally.
2. Open a terminal in the extracted `walmart-data-engineering-final` folder.
3. Initialise Git and create the branch if this is a new repository:

   ```bash
   git init -b feature/walmart-data-engineering-final
   git add .
   git status
   ```

4. Inspect `git status` and confirm no `.env`, `profiles.yml`, logs, tokens or generated dbt artifacts are staged.
5. Commit:

   ```bash
   git commit -m "Prepare Walmart data engineering portfolio project"
   ```

6. Create an empty GitHub repository, then add its URL and push:

   ```bash
   git remote add origin https://github.com/<your-username>/<your-repository>.git
   git push -u origin feature/walmart-data-engineering-final
   ```

Do not push credentials. The project previously contained a Databricks token in a local `profiles.yml`; revoke that token and issue a replacement before publishing or running the project again.
