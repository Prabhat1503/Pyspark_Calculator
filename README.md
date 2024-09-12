# PySpark Calculator

This project demonstrates how to create and use User Defined Functions (UDFs) in PySpark for basic arithmetic operations. The operations include addition, subtraction, multiplication, and division.

## Repository Structure

- **`Calculator.py`**: Python script containing the PySpark code for performing arithmetic operations using UDFs.

## Description

The script performs the following tasks:

1. **Set Up Spark Session**: Initializes a Spark session to work with PySpark.
2. **Define Calculator Functions**: Defines Python functions for basic arithmetic operations (add, subtract, multiply, divide).
3. **Register Functions as UDFs**: Converts the Python functions into UDFs so they can be used within Spark DataFrames.
4. **Create Sample DataFrame**: Constructs a sample DataFrame with two columns (`x` and `y`).
5. **Apply UDFs**: Uses the UDFs to perform arithmetic operations and adds the results as new columns to the DataFrame.
6. **Display Results**: Shows the original and updated DataFrame with results of arithmetic operations.

## Code Overview

### Step 1: Set Up Spark Session

```python
spark = SparkSession.builder.appName("Calculator").getOrCreate()
```

### Step 2: Define Calculator Functions

```python
def add(x, y):
    if x is not None and y is not None:
        return float(x) + float(y)
    return None

def subtract(x, y):
    if x is not None and y is not None:
        return float(x) - float(y)
    return None

def multiply(x, y):
    if x is not None and y is not None:
        return float(x) * float(y)
    return None

def divide(x, y):
    if x is not None and y is not None and y != 0:
        return float(x) / float(y)
    return None
```

### Step 3: Register Functions as UDFs

```python
add_udf = udf(add, DoubleType())
subtract_udf = udf(subtract, DoubleType())
multiply_udf = udf(multiply, DoubleType())
divide_udf = udf(divide, DoubleType())
```

### Step 4: Create a Sample DataFrame

```python
data = [(10, 5), (20, 0), (15, 3), (50, 25)]
columns = ["x", "y"]
df = spark.createDataFrame(data, columns)
df = df.withColumn("x", df["x"].cast(DoubleType()))
df = df.withColumn("y", df["y"].cast(DoubleType()))
```

### Step 5: Apply Calculator Functions

```python
df_with_addition = df.withColumn("addition", add_udf(df["x"], df["y"]))
df_with_subtraction = df_with_addition.withColumn("subtraction", subtract_udf(df["x"], df["y"]))
df_with_multiplication = df_with_subtraction.withColumn("multiplication", multiply_udf(df["x"], df["y"]))
df_with_division = df_with_multiplication.withColumn("division", divide_udf(df["x"], df["y"]))
```

### Step 6: Show the Results

```python
df_with_division.show()
```

## Example Output

The script outputs a DataFrame with the following columns:
- `x`: First operand
- `y`: Second operand
- `addition`: Result of `x + y`
- `subtraction`: Result of `x - y`
- `multiplication`: Result of `x * y`
- `division`: Result of `x / y` (with error handling for division by zero)

  ![image](https://github.com/user-attachments/assets/d982c59f-e2d0-4ffa-a3fc-798c519b0553)


## Requirements

- Apache Spark
- PySpark

## Usage

1. Ensure you have Apache Spark and PySpark installed.
2. Save the provided code to a file named `Calculator.py`.
3. Run the script using a Spark-submit command or a compatible environment.

## Additional Notes

- This script provides a basic example of using UDFs in PySpark. Modify the code as needed to fit specific use cases or datasets.
- Handle any potential errors or edge cases according to your application's requirements.

---

This README file provides a clear overview of the functionality and usage of the PySpark script for performing basic arithmetic operations using UDFs.
