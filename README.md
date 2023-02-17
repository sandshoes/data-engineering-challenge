# Amenitiz © Data Engineer challenge 1

# Explanation of my approach
There are a few things I've done that may escape the expected way of doing things, especially when it comes to API requests.
1. Didn't save the API results in a source json file to then process it. The repo makes an API call to fetch information every time. This could be easily changed.
2. I have built a UDF to call the API for details. If records where long it could be pretty expensive but in this case it was not.
3. I didn't use advanced Spark as I had not cluster available so didn't take advantage of RDD and lazy processing of tasks. 
4. Was pretty dumb and added my API_KEY in a previous commit. Cannot cancel it and request a new one so I will close my account once the process has been reviewed.
5. Adding request error handling or quarantining records would be a massive improvement to the project


# Instructions
Hello :wave: dear candidate. We are glad you get this far in your journey of becoming part of Amenitiz as a Data Engineer.

You should have forked this repository from the Amenitiz organization in GitHub

:exclamation: In order to start this challenge you will need some actions previously executed:
- [ ] Create an account (or two) at [OpenTripMap](https://opentripmap.io/product). Do not worry, it is free, but take 
care of their API rate limits.
- [ ] Had your favourite IDE prepared to code in [Python](https://www.python.org/downloads/release/python-3716/) 
(preferred version **3.7** or higher). We encourage [PyCharm](https://www.jetbrains.com/pycharm/) usage
- [ ] Had set up a Virtual Environment to run the project and its tests ([venv](https://docs.python.org/3/library/venv.html) 
is preferred)
- [ ] For dataframe based operations, please either use [Pandas](https://pypi.org/project/pandas/) or 
[PySpark](https://pypi.org/project/pyspark/) libraries

:alarm_clock: Depending on your level of experience the challenge might take more or less time. A Senior profile could finish it in
a couple of hours... anyway, just let us know how much time you need to deliver it

:envelope: The delivery method will consist in opening a Pull Request from your forked repository to the main repository in GitHub

# Suggestions

1. Not all the functional requirements have to be implemented in the order they have been written. There are dozens of ways
of implementing solutions to the challenge, there is no single and unique "right" implementation. 
2. If we were you, we will probably stick to the "preferred" options offered
3. The words must/should and their negative forms are being applied in this whole document as the [RFC-2119](https://www.ietf.org/rfc/rfc2119.txt) standard states
4. The Non-functional requirements expressed are minimal, we hope to appreciate the inclusion of obvious other ones 
that should be always present in every project :eyes:
5. From all the Software Engineering principles that exist, the most loved one in Amenitiz is the
[KISS](https://en.wikipedia.org/wiki/KISS_principle) principle... if you know what we mean...
6. If you have doubts please, do not hesitate to contact us asking anything you need to complete the challenge comfortably
7. How you use git (commit messages, branches, etc.) is something we are going to check, try to be coherent and tidy
8. Just in case, we let you know you can have several Python versions available within your Operating System thanks to tools like [pyenv](https://github.com/pyenv/pyenv)

# Requirements
There we go!

## Functional requirements
**FRQ-01**: 

Extract 2500 objects from *OpenTripMap*
  - Language must be: english
  - Kinds should be: `accomodations` (yes, the typo is theirs) 
  - Format must be: `json` 
  - Minimum longitude: `2.028471` 
  - Maximum longitude: `2.283903` 
  - Minimum latitude: `41.315758` 
  - Maximum latitude: `41.451768`
  
**FRQ-02**: 
  
Transform the JSON array obtained from *OpenTripMap* to a Pandas or PySpark dataframe.

Make sure the dataframe does not contain complex data types (array, struct or map)

**FRQ-03**:

Filter those records that include the word "*skyscrapers*" within its kinds

**FRQ-04**:

Add a new dimension `kinds_amount`, which is the count of kinds of a particular place

**FRQ-05**:

For every record in the dataframe add the following dimensions extracted from OpenTripMap details API information 

- stars
- address (all fields)
- url
- image
- wikipedia (just the url)

:warning: keep in mind this dimensions must not be complex

**FRQ-06**:

Once the dataframe has been properly transformed according to the previous functional requirements,
save it into a cvs file with headers (i.e. *places_output.csv*)

**FRQ-07**:

As a bonus, which means this one is a nice to have and not mandatory, plot into a .jpg file the area where we have 
searched this places as well as their positions (as leaflets or red dots, for example) 


## Non-functional requirements

**NRQ-01**:

Codebase must follow a concrete structure. Either define one on your own (we would like to know the decisions made 
regarding this choice) or use a preexisting/standard template 

**NRQ-02**:

Project must include a requirements.txt (filled with **all** the required dependencies) file and a .gitignore file 
(to prevent committing files that are not sources) 
If you have doubts regarding how to make a proper .gitignore file, search for .gitignore templates around the Internet

**NRQ-03**:

Code style must follow [PEP 8](https://peps.python.org/pep-0008/) convention

**NRQ-04**:

Provide Unit Tests ([unittest](https://docs.python.org/3/library/unittest.html) preferred). We highly encourage these 
following the [AAA](https://jamescooke.info/arrange-act-assert-pattern-for-python-developers.html) approach

---

That's all, we wish you the best! :v: