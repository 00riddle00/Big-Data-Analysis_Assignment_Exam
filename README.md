<!-- vim: set ft=markdown tw=88 nu ai et ts=2 sw=2: -->

# Exam Assignment: Detection of Vessel Collisions

**Course:** Big Data Analysis (10 ECTS), VU MIF, Spring 2026

**Study program:** MSc Data Science

**Student:** [Tomas Giedraitis](https://github.com/00riddle00)

## Table of Contents:

<!--toc:start-->
- [Exam Assignment: Detection of Vessel Collisions](#exam-assignment-detection-of-vessel-collisions)
  - [Table of Contents:](#table-of-contents)
- [Part I — Assignment Specification](#part-i--assignment-specification)
  - [Objective](#objective)
  - [Technical Requirements](#technical-requirements)
  - [Assignment Description](#assignment-description)
  - [Deliverables and Output](#deliverables-and-output)
    - [Source Code Repository](#source-code-repository)
    - [Containerization (Docker)](#containerization-docker)
    - [Written Report](#written-report)
    - [Results](#results)
    - [Visualization](#visualization)
  - [Evaluation Criteria](#evaluation-criteria)
  - [Additional — Dataset Schema](#additional--dataset-schema)
- [Part II — Implementation](#part-ii--implementation)
<!--toc:end-->

# Part I — Assignment Specification

## Objective

The objective of this examination is to evaluate your ability to process large-scale
temporal and spatial data. You are required to identify two vessels that have collided
(or experienced the closest possible physical proximity indicating a collision) within a
specified marine area. You must visualize their respective trajectories `10 minutes`
prior to and `10 minutes` following the time of collision.

## Technical Requirements

- **Language:** `Python 3.x`
- **Framework:** You must utilize a big data processing framework (e.g., `Apache
  Spark`/`PySpark`). Processing the raw data exclusively with standard `Pandas` will not
  be accepted.
- **Environment:** The solution must be fully containerized using `Docker`.
- **Version Control:** `Git` (submission via repository).
- **Data Source:** Danish AIS Data ([http://aisdata.ais.dk/](http://aisdata.ais.dk/))

## Assignment Description

The provided dataset contains raw Automatic Identification System (`AIS`) records, which
include details such as `MMSI` (Maritime Mobile Service Identity), timestamps, latitude,
longitude, and navigational status.

Your analysis must adhere to the following constraints:

- **Timeframe:** Restrict your data processing to the exact period of `December 1,
  2021`, to `December 31, 2021`.
- **Geographic Area:** Filter the dataset to isolate vessels operating within a
  `50-nautical-mile (nm)` radius of a center coordinate located at `Latitude:
  55.225000`, `Longitude: 14.245000`.
- **Vessel State:** You are looking specifically for moving vessels that intersect in
  time and space, resulting in a collision. You must implement logic to identify and
  filter out stationary vessels (e.g., ships at anchor or safely docked adjacent to one
  another).
- **Data Integrity:** `AIS` data frequently contains errors. You must account for and
  filter out GPS anomalies and data noise. This is critical to ensure that a sudden jump
  in GPS coordinates is not falsely identified as a collision.

## Deliverables and Output

Students must submit their work incorporating the following components:

### Source Code Repository

Provide a link to a `Git` repository (e.g., `GitHub`, `GitLab`) containing your complete
solution. The code must be well-documented, explaining the rationale behind your big
data transformations, your spatial filtering approach, and how you optimized the
computational cost of detecting the collision.

### Containerization (Docker)

The repository must include a valid `Dockerfile` (and `docker-compose.yml` if
applicable). In addition, you must submit the compiled Docker image (e.g., via a link to
a public registry like `Docker Hub`, or as an exported `.tar` file as specified by the
submission portal). The repository must contain a `README.md` file with explicit
commands on how to build and execute the container.

### Written Report

A brief report (which may be included as a Markdown file in the repository) discussing
your findings. This should detail your methodology for defining and excluding data
noise, verifying the collision, and any insights regarding the dataset or your
computational strategy.

### Results

The final output of your code must explicitly state the `MMSI` numbers, the names of the
two collided vessels, and the exact timestamp and coordinates of the collision event.

### Visualization

A plotted map visualizing the trajectory of the two identified vessels over a
`20-minute` window (exactly `10 minutes` before and `10 minutes` after the collision
time). This visualization should be generated or saved as an output when the `Docker`
container is executed.

## Evaluation Criteria

Your submission will be graded according to the following metrics:

- **Reproducibility and Architecture:** The successful containerization of the
  application. The `Docker` image must build and execute correctly based on your
  provided instructions.
- **Computational Efficiency:** The appropriate and efficient use of big data
  transformations and actions. Inefficient distance calculations (such as an unoptimized
  Cartesian product) will result in a lower grade.
- **Data Engineering:** The correct implementation of data loading, cleaning, and
  preprocessing.
- **Accuracy:** The correct identification of the collided vessel pair, accurate spatial
  distance calculations, and successful elimination of noise.
- **Documentation and Visualization:** The clarity of your code comments, the depth of
  your written analysis, and the accuracy of your trajectory visualization.

---

## Additional — Dataset Schema

The `AIS` `CSV` files contain `26 columns`:

| #  | Columns in `*.csv` file        | Format                                                                                                       |
| -- | ------------------------------ | ------------------------------------------------------------------------------------------------------------ |
| 1  | Timestamp                      | Timestamp from the AIS basestation, format: `31/12/2015 23:59:59`                                            |
| 2  | Type of mobile                 | Describes what type of target this message is received from (class A AIS Vessel, Class B AIS vessel, etc)    |
| 3  | MMSI                           | MMSI number of vessel                                                                                        |
| 4  | Latitude                       | Latitude of message report (e.g. `57,8794`)                                                                  |
| 5  | Longitude                      | Longitude of message report (e.g. `17,9125`)                                                                 |
| 6  | Navigational status            | Navigational status from AIS message if available, e.g.: `Engaged in fishing`, `Under way using engine`, mv. |
| 7  | ROT                            | Rot of turn from AIS message if available                                                                    |
| 8  | SOG                            | Speed over ground from AIS message if available                                                              |
| 9  | COG                            | Course over ground from AIS message if available                                                             |
| 10 | Heading                        | Heading from AIS message if available                                                                        |
| 11 | IMO                            | IMO number of the vessel                                                                                     |
| 12 | Callsign                       | Callsign of the vessel                                                                                       |
| 13 | Name                           | Name of the vessel                                                                                           |
| 14 | Ship type                      | Describes the AIS ship type of this vessel                                                                   |
| 15 | Cargo type                     | Type of cargo from the AIS message                                                                           |
| 16 | Width                          | Width of the vessel                                                                                          |
| 17 | Length                         | Length of the vessel                                                                                         |
| 18 | Type of position fixing device | Type of positional fixing device from the AIS message                                                        |
| 19 | Draught                        | Draught field from AIS message                                                                               |
| 20 | Destination                    | Destination from AIS message                                                                                 |
| 21 | ETA                            | Estimated Time of Arrival, if available                                                                      |
| 22 | Data source type               | Data source type, e.g. AIS                                                                                   |
| 23 | Size A                         | Length from GPS to the bow                                                                                   |
| 24 | Size B                         | Length from GPS to the stern                                                                                 |
| 25 | Size C                         | Length from GPS to starboard side                                                                            |
| 26 | Size D                         | Length from GPS to port side                                                                                 |

---

# Part II — Implementation
