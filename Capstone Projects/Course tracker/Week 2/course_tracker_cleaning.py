import pandas as pd
import numpy as np

# ---------------------------
# STEP 1: Load CSV Data
# ---------------------------
enrollment_df = pd.read_csv("enrollments.csv")
progress_df = pd.read_csv("progress.csv")

print("Enrollment Data:")
print(enrollment_df.head())
print("\nProgress Data:")
print(progress_df.head())

# ---------------------------
# STEP 2: Clean Data
# ---------------------------

# Convert date columns to datetime
enrollment_df['enrollment_date'] = pd.to_datetime(enrollment_df['enrollment_date'], errors='coerce')
progress_df['last_updated'] = pd.to_datetime(progress_df['last_updated'], errors='coerce')

# Check for missing values
print("\nMissing values in Enrollment:")
print(enrollment_df.isnull().sum())

print("\nMissing values in Progress:")
print(progress_df.isnull().sum())

# Fill missing completion % with 0
progress_df['completion_percentage'] = progress_df['completion_percentage'].fillna(0)

# Clip progress between 0 and 100
progress_df['completion_percentage'] = np.clip(progress_df['completion_percentage'], 0, 100)

# ---------------------------
# STEP 3: Merge Both Datasets
# ---------------------------
# Join on enrollment_id to link students and course IDs to progress
merged_df = pd.merge(enrollment_df, progress_df, on='enrollment_id', how='inner')

print("\nMerged Data Sample:")
print(merged_df.head())

# ---------------------------
# STEP 4: Calculate Average Progress
# ---------------------------

# Overall average progress
overall_avg = merged_df['completion_percentage'].mean()
print(f"\nOverall Average Completion: {overall_avg:.2f}%")

# Average by course
avg_by_course = merged_df.groupby('course_id')['completion_percentage'].mean().reset_index()
avg_by_course.columns = ['Course ID', 'Average Completion (%)']

print("\n Average Completion by Course:")
print(avg_by_course)

# ---------------------------
# STEP 5: Export Cleaned Data (Optional)
# ---------------------------
# Save cleaned and merged data for later use
merged_df.to_csv("cleaned_progress_data.csv", index=False)
avg_by_course.to_csv("course_completion_summary.csv", index=False)

print("\n Cleaned data and course summary saved to CSV.")
