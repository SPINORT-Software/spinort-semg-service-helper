#!/bin/bash

# Pull the latest changes from the Git repository
git pull

# Print a message indicating the success or failure of the pull operation
if [ $? -eq 0 ]; then
    echo "Git pull successful."
else
    echo "Error: Unable to pull from Git repository."
fi