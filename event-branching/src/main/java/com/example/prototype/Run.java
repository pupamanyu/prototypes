/*
 * Copyright 2024

 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at

 * http://www.apache.org/licenses/LICENSE-2.0

 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.example.prototype;

import com.example.consumer.EventConsumer;
import com.example.producer.EventProducer;

public class Run {

  public static void main(String[] args) {
    if (args.length != 2 || !args[0].equals("--run")) {
      System.out.println("Usage: java Run --run <producer|consumer>");
      return;
    }

    switch (args[1]) {
      case "producer":
        EventProducer.run();
        break;

      case "consumer":
        EventConsumer.run();
        break;

      default:
        System.out.println("Invalid value for --run. Use 'producer' or 'consumer'.");
        break;
    }
  }
}
