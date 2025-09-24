# Music Streaming Analysis Using Spark Structured APIs

## Overview
This Spark program processes music listening logs along with song metadata to extract useful insights. It first determines each user’s favourite genre by counting the number of plays per genre and selecting the most listened one. Next, it calculates the average listening time for every song by computing the average duration in seconds. The program then computes a genre loyalty score for each user, which is the proportion of plays belonging to their top genre, and filters out users with a score greater than 0.8. Finally, it identifies “night owl” users who have at least five plays between 12 AM and 5 AM. The results of all four tasks are stored as CSV files in the `output/` folder, with a separate subfolder created for each task.
## Dataset Description
The program works with two datasets. The first one is the listening logs, which has information about which user played which song, the timestamp of the play, and how long they listened in seconds. The second dataset is the songs metadata, which contains details about each song such as its ID, title, artist, genre, and mood. By combining these two datasets, we can analyze both user behavior and song information together.
## Repository Structure
The repository mainly has the main.py file which contains all the Spark code to perform the analysis. Along with that, there are the input CSV files (`listening_logs.csv` and `songs_metadata.csv`) that are loaded by the program. The output of the program is saved into a separate folder called `output/`.
## Output Directory Structure
After running the program, the results are stored in the `output/` directory. Inside it, there are four subfolders, one for each task:
```text
output/
├── user_favorite_genres/
├── avg_listen_time_per_song/
├── genre_loyalty_scores/
└── night_owl_users/

```
## Tasks and Outputs
The program performs four main tasks.

1. User’s Favourite Genre – Finds each user’s favorite genre by counting how many times they listened to songs in different genres and selecting the most played one.

2. Average Listen Time per Song – Calculates the average listening time for each song based on the user play history.

3. Genre Loyalty Score – Computes a genre loyalty score for each user, which shows how much they stick to their top genre, and outputs users with a score greater than 0.8.

4. Night Owl Users – Identifies users who listen to music late at night, between 12 AM and 5 AM, and saves those users separately.

All these results are written into their own folders inside the `output/` directory.
## Execution Instructions
## *Prerequisites*

Before starting the assignment, ensure you have the following software installed and properly configured on your machine:

1. *Python 3.x*:
   - [Download and Install Python](https://www.python.org/downloads/)
   - Verify installation:
     ```bash
     python3 --version
     ```

2. *PySpark*:
   - Install using pip:
     ```bash
     pip install pyspark
     ```

3. *Apache Spark*:
   - Ensure Spark is installed. You can download it from the [Apache Spark Downloads](https://spark.apache.org/downloads.html) page.
   - Verify installation by running:
     ```bash
     spark-submit --version
     ```

### *2. Running the Analysis Tasks*

####  *Running Locally*

1. *Generate the Input*:
  ```bash
   python3 datagen.py
   ```

2. **Execute Each Task Using spark-submit**:
   ```bash
     spark-submit main.py
   ```

3. *Verify the Outputs*:
   Check the outputs/ directory for the resulting files:
   ```bash
   ls outputs/
   ```

## Errors and Resolutions
While working on this Spark program I came across a few errors. One common issue was with the round function. I had originally written `sround`, but Spark gave a `NameError` because the function was not imported correctly.

Another error happened when I tried to run the program multiple times. Spark does not allow writing to the same folder unless the mode is set to overwrite. To fix this, I added `.mode("overwrite")` before the write statement so the old results get replaced without errors.

I also noticed that the outputs were being written as multiple part files instead of a single file. To solve this, I added `.coalesce(1)` before `.write` so Spark combines everything into one output file for easier reading.

These small fixes made the program run smoothly and the results were saved correctly in the output folder.