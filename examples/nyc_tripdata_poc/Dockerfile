# Use the official Jupyter Notebook image as the base image
FROM jupyter/scipy-notebook

# Switch to root user to install additional packages
USER root

# Download PostgreSQL JDBC driver
RUN wget -P /usr/local/share/postgresql/ https://jdbc.postgresql.org/download/postgresql-42.7.3.jar

# Switch back to jovyan user for running Jupyter
USER jovyan

# Install additional Python packages
RUN pip install \
    joblib \
    fuzzywuzzy \
    sqlalchemy \
    psycopg2-binary

# Copy the notebook files into the container
COPY . /home/nimbly-dev/work

# Set the working directory
WORKDIR /home/nimbly-dev/work

# Expose the Jupyter Notebook port
EXPOSE 8888

# Start Jupyter Notebook
CMD ["start-notebook.sh", "--NotebookApp.token=''"]