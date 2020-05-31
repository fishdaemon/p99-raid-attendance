import pandas
import fileinput
import re
import numpy
from shutil import copyfile
from sys import platform
import csv

log_file = "raw_log.txt"
data_file = "data_file.txt"
separator = "AAA"
# Date format character
dfc = "%"

if platform == "darwin":
    dfc = "%-"
elif platform == "win32":
    dfc = "%#"

# Will rewrite the log file with a new separator so that data is structured
copyfile(log_file, data_file)
for line in fileinput.input([data_file], inplace=1):
    result = re.search('^\[(?P<date>[A-z]{3} [A-z]{3} \d{1,2} \d\d:\d\d:\d\d \d{4})] (?P<entry>.*)$', line)
    if result is None:
        continue
    print("{} {} {}".format(result.group("date"), separator, result.group("entry")))

df = pandas.read_csv(data_file, sep=separator, parse_dates=[0], engine="python", header=None)
#
df = df.rename(columns={0: 'timestamp'})
df = df.rename(columns={1: 'log_entry'})
#
# log_df.rename()
df['attendance'] = numpy.where(df.log_entry.str.contains('Players on EverQuest:'), True, None)

reports = list()

for row, attendance in df['attendance'].iteritems():
    counter = row
    report_data = list()
    while attendance:

        current_log_entry = str(df.log_entry[counter]).strip()

        player_data = re.search('\[.*] (?P<name>.*) (\(.*\))? <Fires of Heaven>$', current_log_entry)
        if player_data is not None:
            report_data.append(
                {
                    "timestamp_official": df.timestamp[counter],
                    "month": df.timestamp[counter].strftime("{0}b".format(dfc)),
                    "day": df.timestamp[counter].strftime("{0}d".format(dfc)),
                    "time": df.timestamp[counter].strftime("{0}H:{0}M:{0}S".format(dfc)),
                    "tag": "Check number {}".format(len(reports) + 1),
                    "Earned DKP": 5,
                    "name": player_data.group("name")
                })
            print(current_log_entry)
        else:
            print("Junk data:  if this is include names in /who,  chat logs or combat logs something went wrong")
            print(current_log_entry)
        result = re.search('^There (are)|(is) (?P<count>\d+) players? in (?P<zone>.*)\.$', current_log_entry)

        if result is not None:
            print("Attendance in {} @ {}".format(result.group("zone"), df.timestamp[counter]))
            attendance = False
            reports.append(report_data)
        if re.match("^There are no players in .* that match those who filters\.$", current_log_entry):
            attendance = False
        counter += 1

csv_columns = ["timestamp_official", "month", "day", "time", "tag", "Earned DKP", "name"]

document_data = list()

for tag in reports:
    document_data.extend(tag)

csv_file = "report.csv"
try:
    with open(csv_file, 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
        writer.writeheader()
        for data in document_data:
            writer.writerow(data)
except IOError:
    print("I/O error")
