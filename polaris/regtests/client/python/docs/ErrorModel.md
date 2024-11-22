<!--

 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.

-->
# ErrorModel

JSON error payload returned in a response with further details on the error

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**message** | **str** | Human-readable error message | 
**type** | **str** | Internal type definition of the error | 
**code** | **int** | HTTP response code | 
**stack** | **List[str]** |  | [optional] 

## Example

```python
from polaris.catalog.models.error_model import ErrorModel

# TODO update the JSON string below
json = "{}"
# create an instance of ErrorModel from a JSON string
error_model_instance = ErrorModel.from_json(json)
# print the JSON string representation of the object
print(ErrorModel.to_json())

# convert the object into a dict
error_model_dict = error_model_instance.to_dict()
# create an instance of ErrorModel from a dict
error_model_from_dict = ErrorModel.from_dict(error_model_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)

