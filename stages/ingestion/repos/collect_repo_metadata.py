import itertools
import os
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
from collections import defaultdict
import subprocess
from servicess.git import checkout_tag


def get_repo_tags(author, repo_name):
    repo_url = f"https://github.com/{author}/{repo_name}.git"
    result = subprocess.run(['git', 'ls-remote', '--tags', repo_url], capture_output=True, text=True)
    tags = [line.split('refs/tags/')[-1] for line in result.stdout.splitlines()]
    return sorted(tags, key=lambda v: [int(x) for x in v.split('.')])


def collect_file_metadata(path):
    file_counts = defaultdict(int)
    line_counts = defaultdict(int)
    file_categories = {}

    for root, dirs, files in os.walk(path):
        for file in files:
            file_path = os.path.join(root, file)
            _, extension = os.path.splitext(file)
            extension = extension.lower()[1:] if extension else 'no_extension'

            # Categorize the file
            if 'test' in file.lower() or 'test' in root.lower():
                category = 'test'
            elif 'doc' in file.lower() or 'doc' in root.lower():
                category = 'doc'
            else:
                category = 'other'

            # Count files
            file_counts[extension] += 1
            file_categories[extension] = category

            # Count lines
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    line_count = sum(1 for line in f)
                    line_counts[extension] += line_count
            except Exception:
                # If we can't read the file, we'll just skip counting its lines
                pass

    return {
        'file_counts': dict(file_counts),
        'line_counts': dict(line_counts),
        'file_categories': file_categories
    }


def process_repo_tags(author, repo_name, tags):
    all_metadata = []
    for tag in tags:
        path = checkout_tag(author, repo_name, tag)
        metadata = collect_file_metadata(path)
        metadata['author'] = author
        metadata['repo'] = repo_name
        metadata['tag'] = tag
        all_metadata.append(metadata)
    return all_metadata


def create_dataframe(all_metadata):
    rows = []
    for metadata in all_metadata:
        for extension, count in metadata['file_counts'].items():
            rows.append({
                'author': metadata['author'],
                'repo': metadata['repo'],
                'tag': metadata['tag'],
                'extension': extension,
                'file_count': count,
                'line_count': metadata['line_counts'].get(extension, 0),
                'category': metadata['file_categories'].get(extension, 'other')
            })
    return pd.DataFrame(rows)


def save_dataframe(df, author, repo_name):
    os.makedirs('../../../metadata', exist_ok=True)
    df.to_csv(f'./metadata/{author}_{repo_name}_metadata.csv', index=False)


def load_dataframe(author, repo_name):
    file_path = f'./metadata/{author}_{repo_name}_metadata.csv'
    if os.path.exists(file_path):
        return pd.read_csv(file_path)
    return None


def create_interactive_plot(df):
    fig = make_subplots(rows=2, cols=1, subplot_titles=("File Counts", "Line Counts"))

    categories = ['test', 'doc', 'other']
    colors = {'test': 'blue', 'doc': 'green', 'other': 'red'}

    # Get unique tags
    unique_tags = df['tag'].unique()

    # Create traces for each tag
    for tag in unique_tags:
        df_tag = df[df['tag'] == tag]

        for i, count_type in enumerate(['file_count', 'line_count']):
            for category in categories:
                df_category = df_tag[df_tag['category'] == category]

                fig.add_trace(
                    go.Bar(
                        x=df_category['extension'],
                        y=df_category[count_type],
                        name=f'{category.capitalize()} ({tag})',
                        marker_color=colors[category],
                        legendgroup=category,
                        showlegend=(i == 0)  # Show legend only for the first subplot
                    ),
                    row=i + 1, col=1
                )

    # Update layout
    fig.update_layout(
        barmode='stack',
        height=800,
        title_text="File and Line Counts by Extension and Category",
    )

    fig.update_xaxes(title_text="File Extension", row=2, col=1)
    fig.update_yaxes(title_text="File Count", row=1, col=1)
    fig.update_yaxes(title_text="Line Count", row=2, col=1)

    # Add slider for tags
    steps = []

    for tag in unique_tags:
        visibility_mask = [False] * len(fig.data)

        # Set visibility for the traces corresponding to the current tag
        for trace_idx, trace in enumerate(fig.data):
            if f"({tag})" in trace.name:  # Check if trace corresponds to the current tag
                visibility_mask[trace_idx] = True

        step = dict(
            method="update",
            args=[{"visible": visibility_mask}],
            label=tag
        )
        steps.append(step)

    sliders = [dict(
        active=0,
        currentvalue={"prefix": "Tag: "},
        pad={"t": 50},
        steps=steps
    )]

    # Initialize the first tag as visible
    fig.update_layout(sliders=sliders)
    fig.show()


def create_tag_based_scatter_chart(df):
    # Create subplots: upper for file count, lower for line count
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True,
                        subplot_titles=("File Count by Tag", "Line Count by Tag"))

    # Define symbols for different categories
    category_symbols = {'test': 'circle', 'doc': 'square', 'other': 'diamond'}

    # Get unique file extensions
    unique_extensions = df['extension'].unique()

    # Define a larger color palette
    color_palette = ['blue', 'green', 'red', 'purple', 'orange', 'cyan', 'magenta', 'brown', 'pink', 'grey', 'olive']

    # Cycle through colors if there are more extensions than available colors
    color_cycle = itertools.cycle(color_palette)

    # Assign colors to extensions
    color_map = {}
    for ext in unique_extensions:
        if ext not in color_map:
            color_map[ext] = next(color_cycle)

    # Add scatter points for file_count (upper) and line_count (lower)
    for count_type, row_num in zip(['file_count', 'line_count'], [1, 2]):
        for category in df['category'].unique():
            df_category = df[df['category'] == category]

            fig.add_trace(go.Scatter(
                x=df_category['tag'],
                y=df_category[count_type],
                mode='markers',
                marker=dict(
                    symbol=category_symbols[category],  # Shape based on category
                    color=[color_map[ext] for ext in df_category['extension']],  # Color based on file extension
                    size=12,
                ),
                name=f'{category.capitalize()} - {count_type.capitalize()}',
                legendgroup=category,  # Group by category to ensure a single legend entry per category
                showlegend=(row_num == 1),  # Show legend only for the first row (file count)
                hovertemplate=(
                    f"<b>Category:</b> {category}<br>"
                    f"<b>Extension:</b> %{df_category['extension']}<br>"
                    f"<b>{count_type.capitalize()}:</b> %{df_category[count_type]}"
                )
            ), row=row_num, col=1)

    # Update layout for shared x-axis and legends
    fig.update_layout(
        height=800,
        title_text="File and Line Counts by Tag, Extension, and Category",
        legend_title="File Format (Extension)",
        hovermode='closest'
    )

    # Customize axes labels
    fig.update_xaxes(title_text="Tag", row=2, col=1)
    fig.update_yaxes(title_text="File Count", row=1, col=1)
    fig.update_yaxes(title_text="Line Count", row=2, col=1)

    # Add extensions to the legend by creating hidden traces for each extension
    for ext in unique_extensions:
        fig.add_trace(go.Scatter(
            x=[None], y=[None],
            mode='markers',
            marker=dict(color=color_map[ext], size=12),
            legendgroup=ext,
            showlegend=True,
            name=f'Extension: {ext}',
        ))

    fig.show()


def main():
    author = "scverse"
    repo_name = "scanpy"

    df = load_dataframe(author, repo_name)

    if df is None:
        tags = get_repo_tags(author, repo_name)
        all_metadata = process_repo_tags(author, repo_name, tags)
        df = create_dataframe(all_metadata)
        save_dataframe(df, author, repo_name)

    # create_interactive_plot(df)
    create_tag_based_scatter_chart(df)


if __name__ == "__main__":
    main()
