#### Project Description: Dynamic Software Integration Test Suite around centralized production tool library
#### Project Creator: Nathan Cronk
#### Supervising Professor: Dr. Eugene Wallingford
#### Purpose: CS 4800 and further implementation at John Deere

The specifications of the project include a centralized tool set of functions that are used
to query dataframes based on input to filter a machine population. These functions are used
in our production rule notebooks that are scheduled by .json files to send a predictive/non-
predictive diagnostic when a machine meets a certain criterion. These functions limit the
amount of SQL or Apache PySpark query's we have to write. The whole goal of this project is
to create a test suite of Unit Tests that mimmic some inputs and a dataframe's current
schema, and what the output schema of that dataframe will be. Since I use John Deere
sensitive data, I would have to go the extra step to create data sets, functions, and unit
test files that mimmic the same concept, that then I could apply at a later date
at John Deere for this specific implementation.

#### Steps going forward:
 1. Create 5 or so functions that can be used on a public data set that could mimmic one of
John Deere's data sets
 2. Create an additional unit test file that imports the dependency of the functions and
creates/mimmics potential inputs of the dataframe's current schema, function parameters, and
what the expected schema of the output dataframe will be
 3. Create a Jenkins file that creates a pipeline for testing, and calls each of the unit
test files as a stage. This Jenkins file should run once a pull request on the function file
is created on a branch that is not the master branch
 4. Since we have access to High Performance Clusters on AWS servers at Deere, we may be
able to schedule each unit test as a job and run the unit test stages all in parallel to
synchronously test and determine whether the tests fail or pass
 5. After all of the unit test files pass/run successfully, the pull request will have this
test completed allowing the addition of a reviewer to be added to do one last manual check
before the branches are merged

#### Goals for this project:
 1. Reduce the amount of manual effort to ensure that any changes to our tool library does
not break any downstream files that use this notebook as a dependency
 2. Create the concept of the project for a public/mimmic data set that then can be easily
transformed down the road to the John Deere data sets and tool library to put this process
into place where production files and their jobs are at risk
 3. Integrate a form of Software Engineering test practices into a Data Engineering
implementation
 4. Allow my co-workers and team members to listen in on the presentation at the end of the
semester to see the work I have put in to not only better myself in my academic career, but
also showcase my learned skillset for later job interviews, and how this project affects our
daily tasks

#### Potential areas of help:
 1. Since I am limited to the amount of time I can spend on this project, I may need some
assistance when it comes to writing the Jenkins file and using best practices to create an
efficient test pipeline
 2. Help finding a mimmic data set to build similar functions
    
#### Tools:
 1. Databricks
 2. GitHub for project tracking and pull requests
 3. Jenkins or Azure DevOps (Open for your thoughts on which one to use)
 4. Free and public data set
