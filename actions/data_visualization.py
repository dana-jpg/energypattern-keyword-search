import os

from constants.db import PROJECTS_DB_NAME, DBCollections
from servicess.MongoDBConnection import MongoDBConnection


def get_schema_sample(collection, sample_size=1000):
    pipeline = [{"$sample": {"size": sample_size}},  # {"$project": {"_id": 0}}  # Exclude _id field
                ]
    sample = list(collection.aggregate(pipeline))
    print(sample)

    schema = {}
    for doc in sample:
        for field, value in doc.items():
            if field not in schema:
                schema[field] = set()
            schema[field].add(type(value).__name__)

    return {field: list(types) for field, types in schema.items()}


def determine_chart_type(field_name, data_types):
    if 'int' in data_types or 'float' in data_types:
        return 'histogram'
    elif 'str' in data_types:
        return 'bar_chart'
    elif 'bool' in data_types:
        return 'pie_chart'
    elif 'datetime' in data_types:
        return 'time_series'
    else:
        return 'table'  # Default to table for complex types


import matplotlib.pyplot as plt
import numpy as np
from collections import Counter


def create_hist(collection, collection_name, field_name, sample_size=1000):
    # Get a sample of documents from the collection
    sample = list(collection.aggregate([{"$sample": {"size": sample_size}}, {"$project": {field_name: 1, "_id": 0}}]))

    # Extract the values for the specified field
    values = [doc[field_name] for doc in sample if field_name in doc]

    # Check if we have any valid values
    if not values:
        print(f"No valid data found for field '{field_name}'")
        return

    # Create the histogram
    plt.figure(figsize=(10, 6))

    # Use numpy to calculate optimal number of bins
    num_bins = int(np.sqrt(len(values)))

    plt.hist(values, bins=num_bins, edgecolor='black')
    plt.title(f'Distribution of {field_name}')
    plt.xlabel(field_name)
    plt.ylabel('Frequency')

    # Add some stats to the plot
    plt.text(0.95, 0.95, f"Count: {len(values)}\n"
                         f"Mean: {np.mean(values):.2f}\n"
                         f"Median: {np.median(values):.2f}\n"
                         f"St. Dev: {np.std(values):.2f}", transform=plt.gca().transAxes, verticalalignment='top',
             horizontalalignment='right', bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))

    plt.savefig(f'./visualizations/{collection_name}/{field_name}_histogram.png')
    plt.close()

    print(f"Histogram for '{field_name}' has been saved as '{field_name}_histogram.png'")


def create_bar_chart(collection, collection_name, field_name, sample_size=1000, top_n=10):
    # Get a sample of documents from the collection
    sample = list(collection.aggregate([
        {"$sample": {"size": sample_size}},
        {"$project": {field_name: 1, "_id": 0}}
    ]))

    # Extract the values for the specified field
    values = [str(doc[field_name]) for doc in sample if field_name in doc]

    # Check if we have any valid values
    if not values:
        print(f"No valid data found for field '{field_name}'")
        return

    # Count the occurrences of each value
    value_counts = Counter(values)

    # Get the top N most common values
    top_values = dict(value_counts.most_common(top_n))

    # Create the bar chart
    plt.figure(figsize=(12, 6))
    plt.bar(top_values.keys(), top_values.values())
    plt.title(f'Top {top_n} values for {field_name}')
    plt.xlabel(field_name)
    plt.ylabel('Count')
    plt.xticks(rotation=45, ha='right')

    # Add count labels on top of each bar
    for i, (key, value) in enumerate(top_values.items()):
        plt.text(i, value, str(value), ha='center', va='bottom')

    plt.tight_layout()
    plt.savefig(f'./visualizations/{collection_name}/{field_name}_bar_chart.png')
    plt.close()

    print(f"Bar chart for '{field_name}' has been saved as '{field_name}_bar_chart.png'")


def create_pie_chart(collection, collection_name, field_name, sample_size=1000, top_n=5):
    # Get a sample of documents from the collection
    sample = list(collection.aggregate([
        {"$sample": {"size": sample_size}},
        {"$project": {field_name: 1, "_id": 0}}
    ]))

    # Extract the values for the specified field
    values = [str(doc[field_name]) for doc in sample if field_name in doc]

    # Check if we have any valid values
    if not values:
        print(f"No valid data found for field '{field_name}'")
        return

    # Count the occurrences of each value
    value_counts = Counter(values)

    # Get the top N most common values
    top_values = dict(value_counts.most_common(top_n))

    # Add an 'Others' category if there are more than top_n values
    if len(value_counts) > top_n:
        others = sum(value_counts.values()) - sum(top_values.values())
        top_values['Others'] = others

    # Create the pie chart
    plt.figure(figsize=(10, 8))
    plt.pie(top_values.values(), labels=top_values.keys(), autopct='%1.1f%%', startangle=90)
    plt.title(f'Distribution of {field_name}')

    plt.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle
    plt.tight_layout()
    plt.savefig(f'./visualizations/{collection_name}/{field_name}_pie_chart.png')
    plt.close()

    print(f"Pie chart for '{field_name}' has been saved as '{field_name}_pie_chart.png'")


def visualize(collection_name: DBCollections = DBCollections.Repos_by_category):
    client = MongoDBConnection().get_client()
    db = client[PROJECTS_DB_NAME]
    collection = db[collection_name.value]

    schema = get_schema_sample(collection)
    print(schema)
    chart_types = {field: determine_chart_type(field, types) for field, types in schema.items()}
    print(chart_types)
    if not os.path.exists(f'./visualizations/{collection_name}/'):
        os.makedirs(f'./visualizations/{collection_name}/', exist_ok=True)

    for field, chart_type in chart_types.items():
        if chart_type == 'histogram':
            create_hist(collection, collection_name, field)  # Add similar functions for other chart types
        if chart_type == 'bar_chart':
            create_bar_chart(collection, collection_name, field)
        if chart_type == 'pie_chart':
            create_pie_chart(collection, collection_name, field)
