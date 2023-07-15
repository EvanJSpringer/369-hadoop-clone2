Evan Springer

For all tasks, I used reduce-side joins because of the relatively small amount of data per key. The small amount of data
means I can have internal lists of collected values in the reduce function without worrying about the data being too
large to contain in memory.

For task 1, I started with the CountryCount job. The country map functions takes the hostname_country.csv file where the
hostname is the key and the country is the value, and returns those keys and values. All other map function takes the
line number as the key and the text as the value. In this job, the other map function takes the access.log file and
returns the hostname as the key with a Text "1" as the value. The reducer receives the output from both mappers and
returns the country name as the key and the sum of the "1"s as the value.
I then processed that output with the CountryCountCombine job, whose map function returns the hostname as key with the
sum as the value. The reduce function sums up all the sum values and returns the country as the key with the total sum
as the value.
I then sorted the output in descending order with the SortByValue job, whose map function returns the negated value
as the key and the country as the value. The reduce function then un-negates the value and returns it as the key
with the country as the value.

For task 2, I started with the HostCountByURL job, whose map function creates a HostURLPair and returns that as the key,
and an IntWritable '1' as the value. The reduce function then sums all the '1's, and returns the HostURLPair's values
as Text for the key, and the sum as the value.
I then processed that output with the CountryCountByURL job, which has the same country map function as before, and
another mapper that returns the hostname as the key, with the URL and sum as a Text value. The reduce function then
iterates through each value and determines if it's the country name or not, and returns the country name as the key with
the URL and value as a Text value.
I then sorted the output with the CountryCountSort job, whose map function creates a CountryCountPair as the key and
the whole line's text as the value. The reduce function then writes the country name, the url, and the count in that
order.

For task 3, I started with the CountryByURL job, who has a country mapper as well. The other map function returns the
hostname as the key with the url as the value. The reduce function again determines the country and a list of urls, and
returns each url as a key with the country as the value.
I then processed and sorted that data with the CountryByURLCombiner job, which returns the url as the key with the
country as the value. There is a SortComparator function that sorts the country names. The reduce function accumulates
all of the country values into a list, sorts it, and then outputs it as the value with the url as the key.