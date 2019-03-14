# Data-utils
In an effort to simplify the AI's team work, this repo will be used to seamlessly acquire the data from the databases.

### Versioning 
The first version will have you copy the code into your own repo 

The second version will most likely be a pip package where depending on what module (let's say the s3) you wish to use, you'll have different setup steps

#### This is what I envision the second version to be
```py
from data_utils import s3

# It will require those 3 variables to create a connection to the bucket on z3
s3.setup(aws_key=os.getenv('AWS_KEY'), secret_key=os.getenv('SECRET_KEY'), bucket=os.getenv('BUCKET'))

# extract_to_csv will have the compression set to 'gzip' by default
s3.extract_to_csv(link='put/in/your/link')
```