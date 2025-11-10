# Web

The Web part is composed of two sections:  
- **Front End**  
- **Back End**

---

# Front End

The front end relies on a single file: [Challengers_Wannabe.html](./Front_end/Challengers_Wannabe.html).  

This file references images stored on AWS S3.  

The reason for having a single file is that I am a Data Engineer, and I focused on functionality rather than a full-fledged front-end architecture.

---

# Back End

The back end is powered by AWS Lambda. Python was chosen because, as a Data Engineer, I have no interest in learning JavaScript or other web-related languages.  

For easy deployment, a [zip package](./Back_end/lambda_deployment_package.zip) of the project is provided. If you make modifications to the code, remember to rebuild the zip file and compile the sources using a Linux-compatible environment.  

The required libraries are listed in [requirements.txt](./Back_end/requirements.txt).  

The data used by the code is stored in the [data/](./Back_end/data) folder.  

Since the Riot Games API key has strict rate limits and cannot retrieve a full year of match history, some [POC data](./Back_end/poc_games/) has already been downloaded for demonstration purposes. You will need to update [lambda_function.py](./Back_end/lambda_function.py) to remove the POC data when using live API queries.
