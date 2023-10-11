# Downloading and Unzipping a Dataset from Google Drive

In this document, we will guide you through the process of downloading a dataset from a Google Drive link and unzipping it in the same workspace or folder after cloning a GitHub directory. Follow these steps to achieve this:

## Steps

### 1. Clone the GitHub Repository

Open a terminal or command prompt and navigate to the directory where you want to clone the GitHub repository. Use the following command to clone the repository:

```
git clone <repository_url>
```

Replace `<repository_url>` with the URL of the GitHub repository you want to clone.

### 2. Access the Google Drive Dataset

Open a web browser and navigate to the Google Drive link where the dataset is hosted. The link should look like this:

[https://drive.google.com/file/d/1Y8ym6XA-iASWjxxKwNcMBJvvcjWllvo7/view?usp=sharing](https://drive.google.com/file/d/1Y8ym6XA-iASWjxxKwNcMBJvvcjWllvo7/view?usp=sharing)

### 3. Download the Dataset

To download the dataset from Google Drive, follow these steps:

-   Click on the "Open with" button at the top of the page.
-   In the dropdown menu, select "Google Drive."
-   A new tab will open, showing the dataset. Click the "Download" button (arrow pointing down) to download the dataset to your computer.

### 4. Move the Downloaded File

After downloading the dataset, locate the downloaded file (e.g., `in.zip`) in your computer's download folder or the default download location.

### 5. Unzip the Dataset

Now, let's unzip the dataset and place it in the same workspace or folder where you cloned the GitHub repository. Follow these steps:

-   Open a terminal or command prompt.
-   Navigate to the directory where you cloned the GitHub repository. Use the `cd` command to change to the appropriate directory.

```
cd  <repository_directory>
```
Replace `<repository_directory>` with the actual directory where you cloned the repository.

-   Use the following command to unzip the downloaded dataset into the same directory:

```
unzip /path/to/downloaded/in.zip -d ./workspace/in
```

Replace `/path/to/downloaded/in.zip` with the actual path to the downloaded `in.zip` file.

### 6. Verify Dataset

You can now verify that the dataset has been successfully unzipped into the `workspace/in` directory.

Congratulations! You have successfully downloaded and unzipped the dataset from the provided Google Drive link and placed it in the same workspace or folder as your cloned GitHub repository.
