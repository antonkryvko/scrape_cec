# Scrape CEC
It is a collection of scripts to scrape any information you may need as a data journalist from Central Election Commitee of Ukraine website. In the future I am going to make it a Python module with command-line interface.
## Usage
TODO: Currently, command-line interface is via development. I'm trying to find a way how to get settings from a json-config in an OOP-paradigm. Another puzzle is to connect candidates' and results' submodules.

So, temporarly just start a script of year and election type you need:
```python3 get_constituencies_candidates_2019.py
python3 get_party_candidates_2019.py
```
### Installation
In order to use a program you should have Python3, git and pip3 installed. It will also be a good idea to create and use a separate virtual environment.
#### Linux (Debian-based)
```bash
sudo apt install python3-pip git
pip3 install virtualenv
git clone https://github.com/antonkryvko/scrape_cec
cd scrape_cec
python3 -m venv scrape_cec_venv
source scrape_cec_venv/bin/activate
pip3 install -r requirements.txt
```
