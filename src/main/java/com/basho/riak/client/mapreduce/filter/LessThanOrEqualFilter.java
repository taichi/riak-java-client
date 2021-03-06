/*
 * This file is provided to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.basho.riak.client.mapreduce.filter;

import org.json.JSONException;
import org.json.JSONArray;

public class LessThanOrEqualFilter implements MapReduceFilter {
    private MapReduceFilter.Types type = MapReduceFilter.Types.FILTER;
    private JSONArray args = new JSONArray();
    
    public LessThanOrEqualFilter(String lessThanOrEqualTo) {
        args.put("less_than_eq");
        args.put(lessThanOrEqualTo);
    }
    
    public LessThanOrEqualFilter(int lessThanOrEqualTo) {
        args.put("less_than_eq");
        args.put(lessThanOrEqualTo);
    }
    
    public LessThanOrEqualFilter(double lessThanOrEqualTo) throws JSONException {
        args.put("less_than_eq");
        args.put(lessThanOrEqualTo);
    }
    
    public JSONArray toJson() {
        return args;
    }
}
