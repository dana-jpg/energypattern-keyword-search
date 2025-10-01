#!/bin/bash

#website_url="https://cloud.google.com/architecture/framework/"
#wget -r -l 2 -np --wait 1 --html-extension $website_url

website_name="cloud.google.com"
folder="$website_name\architecture\framework"
#qas=(ls -d "$folder")
qas=(cost-optimization operational-excellence performance-optimization perspectives reliability security system-design)
for qa in "${qas[@]}"
do
    echo $qa _ "$qa/*.html"
    find . -type f -path "*/$qa/*.html" -exec echo {} +
    find . -type f -path "*/$qa/*.html" -exec cat {} + > "$website_name-$qa.html"
done

