#include <iostream>
#include <fstream>
#include <string>
#include <sstream>
#include <vector>
#include <unordered_map>
#include <thread>
#include <queue>
#include <mutex>
#include <chrono>
#include <ctime>
#include <iomanip>
#include <sstream>
#include <algorithm>

using namespace std;
mutex mtx;

//---------------------------FUNCTION FOR PRIORITY QUEUE----------------------------------------------------
struct CompareVectors {
    bool operator()(const vector<string>& a, const vector<string>& b) const {
        return stof(a[9]) > stof(b[9]);  // greater-than comparison for min-heap behavior
    }
};

//---------------------------MAKING THE TIME-STAMP----------------------------------------------------------
string getCurrentTimestamp() {
    // Get the current time
    auto now = chrono::system_clock::now();
    time_t time = chrono::system_clock::to_time_t(now);

    // Convert time to struct tm
    struct tm timeInfo;
    try { // survive from unexpected localtime_s function errors
        localtime_s(&timeInfo, &time);
    } catch (const exception& e) {
        return "Error getting timestamp";
    }

    // Format the timestamp
    stringstream ss;
    ss << put_time(&timeInfo, "%Y%m%d-%H%M%S")
       << '.' << chrono::duration_cast<chrono::milliseconds>(now.time_since_epoch()).count() % 1000;

    return ss.str();
}

//--------------------------------FUCTION TO VERIFICATION--------------------------------------------------------------------
int isRejected(string field, int column_number) {
    float price;
    int quantity;

    // Check if any required field is missing
    if (field.empty()) {
        return 400;
    }

    if (column_number == 1) { // instrument column
        if (field != "Rose" && field != "Lavender" && field != "Lotus" && field != "Tulip" && field != "Orchid") {
            return 401;
        }
    }
    else if (column_number == 2) { // buy sell column
        if (field != "1" && field != "2") {
            return 402;
        }
    }
    else if (column_number == 3) { // quantity column
        try {
            quantity = stoi(field);
        } catch (...) {
            return 403;
        }
        if (quantity % 10 != 0 || quantity >= 1000) {
            return 403;
        }
    }
    else if (column_number == 4) { // price column
        try {
            price = stof(field);
        } catch (...) {
            return 404;
        }
        if (price <= 0.0) {
            return 404;
        }
    }

    return 200; // if everything OK

}

//---------------------------CLASSIFY THE ORDERS INTO REJECTED AND ACCEPTED--------------------------------------------
void classifyOrders(unordered_map<string, vector<vector<string>>>& flower_fields, vector<string>& row, int error_code) {
    string flowerName = row[2];
    if (error_code == 200) { // if everything OK
        flower_fields[flowerName].push_back(row);
    }
    else { // error occured
        if (error_code == 400) {
            row.push_back("Missing field");
        }
        else if (error_code == 401) {
            row.push_back("Invalid instrument");
        }
        else if (error_code == 402) {
            row.push_back("Invalid side");
        }
        else if (error_code == 403) {
            row.push_back("Invalid size");
        }
        else if (error_code == 404) {
            row.push_back("Invalid price");
        }
        flower_fields["Rejected"].push_back(row);
    }
}


//--------------------------READ CSV FILE FROM USING MEMORY BUFFER--------------------------------------------
void readCsv(const string& filename,
             unordered_map<string, vector<vector<string>>>& flower_fields) {
    ifstream inputFile(filename);
    if (!inputFile.is_open()) {
        cerr << "Error opening file." << endl;
        return;
    }

    // Get the size of the file
    inputFile.seekg(0, ios::end);
    streampos fileSize = inputFile.tellg();
    inputFile.seekg(0, ios::beg);

    // Read the file into a memory buffer
    vector<char> buffer(fileSize);
    inputFile.read(buffer.data(), fileSize);

    inputFile.close(); // Close the file

    // Process the buffer
    istringstream iss(buffer.data());
    string line;
    int count = 0;
    getline(iss, line); // Skip the first line (header)
    while (getline(iss, line)) {
        istringstream lineStream(line);
        int column_number = 0; // count the number of columns in a row
        string field;
        int error_code = 200; // 200 means no error
        vector<string> row = {to_string(++count)};

        while (getline(lineStream, field, ',')) {
            if (column_number > 4) break; // columns should be 5
            if (error_code == 200) {
                error_code = isRejected(field, column_number);
            }
            row.push_back(field); // Store each field
            column_number++;
        }
        if (row.size() != 1) {
            classifyOrders(flower_fields, row, error_code); // classify the orders into rejected and accepted according to the error code
        }
    }
}

//-------------------ASCENDING ORDERED ARRAY OF SELL SIDE ACCORDING TO PRICE-------------------------------------
void addSellArr(vector<vector<string>>& out_arr, const vector<string>& row_arr) {
    float price = stof(row_arr[5]);

    auto insertion_pos = upper_bound(out_arr.begin(), out_arr.end(), price,
        [](float value, const vector<string>& vec) {
            return value < stof(vec[5]); // Compare based on price
        }
    );

    out_arr.insert(insertion_pos, row_arr);
}

//-------------------DESCENDING ORDER ARRAY OF BUY SIDE ACCORDING TO PRICE----------------------------------------
void addBuyArr(vector<vector<string>>& out_arr, const vector<string>& row_arr) {
    float price = stof(row_arr[5]);

    auto insertion_pos = upper_bound(out_arr.begin(), out_arr.end(), price,
        [](float value, const vector<string>& vec) {
            return value > stof(vec[5]); // Compare based on price
        }
    );

    out_arr.insert(insertion_pos, row_arr);
}


//-------------------------------------MAKING THE CSV FILE-------------------------------------------------------
void writeToCsv(priority_queue<vector<string>, vector<vector<string>>, CompareVectors> trade_heap, const string& filename) {
    ofstream outputFile(filename);
    if (!outputFile.is_open()) {
        cerr << "Error opening output file." << endl;
        return;
    }

    // adds the header row first
    vector<string> trade_arr = {"Order ID", "Client Order ID", "Instrument", "Side", "Exec Status", "Quantity", "Price", "Transaction Time", "Reason"}; // header
    for (size_t i = 0; i < trade_arr.size(); ++i) {
        outputFile << trade_arr[i];
        if (i < trade_arr.size() - 1) {
            outputFile << ",";
        }
    }
    outputFile << endl;

    // Write executed orders to the CSV file
    while (!trade_heap.empty()) {
        vector<string> topVector = trade_heap.top();
        trade_heap.pop();

        // Exclude the last element (order flow column), this order flow column is used for make the correct ordering
        topVector.pop_back();

        for (size_t i = 0; i < topVector.size(); ++i) {
            outputFile << topVector[i];
            if (i < topVector.size() - 1) {
                outputFile << ",";
            }
        }
        outputFile << endl;
    }

    // Close the output file
    outputFile.close();

    cout << "CSV file created successfully." << endl;
}

//-------------------------------------INSERT NEW ROW IN PRIORITY QUEUE-----------------------------------------
void insertTradeHeap(priority_queue<vector<string>, vector<vector<string>>, CompareVectors>& trade_heap, vector<string>& row_arr, string exec_status, string order_flow, string timestamp) {
    lock_guard<mutex> lock(mtx); // lock the thread until one thread is done
    vector<string> new_row_arr = {};
    int i = 0;
    if (exec_status == "Reject") i = 1; // because of the reason column order id shift to right side in rejected orders
    new_row_arr.push_back(row_arr[i + 6]); // order id
    new_row_arr.push_back(row_arr[1]); // customer id
    new_row_arr.push_back(row_arr[2]); // instrument
    new_row_arr.push_back(row_arr[3]); // buy or sell
    new_row_arr.push_back(exec_status); // stauts
    new_row_arr.push_back(row_arr[4]); // quantity
    new_row_arr.push_back(row_arr[5]); // price
    new_row_arr.push_back(timestamp); // timestamp
    if (exec_status == "Reject") { 
        new_row_arr.push_back(row_arr[6]); // reason
    } else {
        new_row_arr.push_back("");
    }
    new_row_arr.push_back(order_flow); // order flow number for make the correct ordering
    trade_heap.push(new_row_arr);
    return;
}

//-------------------------------------ALGORITHM FOR THE BUY ORDERS---------------------------------------------------
void processBuyOrders(priority_queue<vector<string>, vector<vector<string>>, CompareVectors>& trade_heap, 
                                                        vector<vector<string>>& sell_arr, vector<string>& row_arr) {
    string timestamp;
    float incrementor = 0.0001; // for make the correct ordering (if one order makes multiple trades, then the order flow number should be increment)
    
    if (sell_arr.size() == 0) { // there are nothing to sell
        timestamp = getCurrentTimestamp();
        insertTradeHeap(trade_heap, row_arr, "New", row_arr[0], timestamp);
    } else {
        if (stof(sell_arr[0][5]) > stof(row_arr[5])) { // sell price is greater than to buy price
            timestamp = getCurrentTimestamp();
            insertTradeHeap(trade_heap, row_arr, "New", row_arr[0], timestamp);
        } else {
            while (row_arr[4] != "0") { // quantity is not zero of the order
                if (sell_arr.size() == 0) { // there are nothing to sell
                    break;
                }
                if (stof(sell_arr[0][5]) > stof(row_arr[5])) { // sell price is greater than to buy price
                    break;
                }
                int sell_quantity = stoi(sell_arr[0][4]);
                int buy_quantity = stoi(row_arr[4]);
                string row_price = row_arr[5];
                string sell_price = sell_arr[0][5];

                // update the transaction price of the order according to the order book
                row_arr[5] = sell_price;
                
                if (sell_quantity > buy_quantity) { // sell quantity is greater than buy quantity
                    sell_arr[0][4] = to_string(buy_quantity); // set the transaction quantity
                    timestamp = getCurrentTimestamp();
                    insertTradeHeap(trade_heap, row_arr, "Fill", row_arr[0], timestamp);
                    row_arr[0] = to_string(stof(row_arr[0]) + incrementor); // increment the order flow number for next row of the same order
                    row_arr[4] = "0"; // set the quantity to zero in order
                    timestamp = getCurrentTimestamp();
                    insertTradeHeap(trade_heap, sell_arr[0], "PFill", row_arr[0], timestamp);
                    row_arr[0] = to_string(stof(row_arr[0]) + incrementor); // increment the order flow number for next row of the same order
                    sell_arr[0][4] = to_string(sell_quantity - buy_quantity); // remaining quantity adds to the order book
                    break;

                } else if (sell_quantity < buy_quantity) { // sell quantity is lesser than buy quantity
                    row_arr[4] = to_string(sell_quantity); // set the transaction quantity
                    timestamp = getCurrentTimestamp();
                    insertTradeHeap(trade_heap, row_arr, "PFill",row_arr[0], timestamp);
                    row_arr[0] = to_string(stof(row_arr[0]) + incrementor); // increment the order flow number for next row of the same order
                    row_arr[5] = row_price; // set the price to the original price for remaining items
                    row_arr[4] = to_string(buy_quantity - sell_quantity); // remaining quantity adds to the order book 
                    timestamp = getCurrentTimestamp();
                    insertTradeHeap(trade_heap, sell_arr[0], "Fill", row_arr[0], timestamp);
                    row_arr[0] = to_string(stof(row_arr[0]) + incrementor); // increment the order flow number for next row of the same order
                    sell_arr.erase(sell_arr.begin()); // remove the completed order from the order book

                } else { // sell quantity is equal to buy quantity
                    
                    timestamp = getCurrentTimestamp();
                    insertTradeHeap(trade_heap, row_arr, "Fill", row_arr[0], timestamp);
                    row_arr[0] = to_string(stof(row_arr[0]) + incrementor); // increment the order flow number for next row of the same order
                    timestamp = getCurrentTimestamp();
                    insertTradeHeap(trade_heap, sell_arr[0], "Fill", row_arr[0], timestamp);
                    row_arr[0] = to_string(stof(row_arr[0]) + incrementor);
                    sell_arr.erase(sell_arr.begin()); // remove the completed order from the order book
                    row_arr[4] = "0"; //order completed, quantity is zero
                    break;
                }
            }
        } 
    }
}

//-------------------------------------ALGORITHM FOR THE SELL ORDERS---------------------------------------------------
void processSellOrders(priority_queue<vector<string>, vector<vector<string>>, CompareVectors>& trade_heap, vector<vector<string>>& buy_arr, vector<string>& row_arr) {
    string timestamp;
    float incrementor = 0.0001; // for make the correct ordering (if one order makes multiple trades, then the order flow number should be increment)
    if (buy_arr.size() == 0) { // there are nothing to buy
        timestamp = getCurrentTimestamp();
        insertTradeHeap(trade_heap, row_arr, "New", row_arr[0], timestamp);
    } else {
        if (buy_arr[0][5] < row_arr[5]) { // buy price is lesser than to sell price
            timestamp = getCurrentTimestamp();
            insertTradeHeap(trade_heap, row_arr, "New", row_arr[0], timestamp);
        } else {
            while (row_arr[4] != "0") { // order quantity is not zero
                
                if (buy_arr.size() == 0) { // there are nothing to buy
                    break;
                }
                if (buy_arr[0][5] < row_arr[5]) { // buy price is lesser than to sell price
                    break;
                }
                int buy_quantity = stoi(buy_arr[0][4]);
                int sell_quantity = stoi(row_arr[4]);
                string row_price = row_arr[5];
                string buy_price = buy_arr[0][5];

                // update the transaction price of the order according to the order book
                row_arr[5] = buy_price;

                if (buy_quantity > sell_quantity) { // buy quantity is greater than sell quantity
                    buy_arr[0][4] = to_string(sell_quantity); // set the transaction quantity
                    timestamp = getCurrentTimestamp();
                    insertTradeHeap(trade_heap, row_arr, "Fill", row_arr[0], timestamp);
                    row_arr[0] = to_string(stof(row_arr[0]) + incrementor); // increment the order flow number for next row of the same order
                    row_arr[4] = "0"; // set the quantity to zero in order, order completed
                    timestamp = getCurrentTimestamp();
                    insertTradeHeap(trade_heap, buy_arr[0], "PFill", row_arr[0], timestamp);
                    row_arr[0] = to_string(stof(row_arr[0]) + incrementor); // increment the order flow number for next row of the same order
                    buy_arr[0][4] = to_string(buy_quantity - sell_quantity); // remaining quantity adds to the order book
                    break;

                } else if (buy_quantity < sell_quantity) { // buy quantity is lesser than sell quantity
                    row_arr[4] = to_string(buy_quantity); // set the transaction quantity
                    timestamp = getCurrentTimestamp();
                    insertTradeHeap(trade_heap, row_arr, "PFill", row_arr[0], timestamp);
                    row_arr[0] = to_string(stof(row_arr[0]) + incrementor); // increment the order flow number for next row of the same order
                    row_arr[5] = row_price; // set the price to the original price for remaining items
                    row_arr[4] = to_string(sell_quantity - buy_quantity); // remaining quantity adds to the order book
                    timestamp = getCurrentTimestamp();
                    insertTradeHeap(trade_heap, buy_arr[0], "Fill", row_arr[0], timestamp);
                    row_arr[0] = to_string(stof(row_arr[0]) + incrementor); // increment the order flow number for next row of the same order
                    buy_arr.erase(buy_arr.begin()); // remove the completed order from the order book

                } else { // buy quantity is equal to sell quantity
                    timestamp = getCurrentTimestamp();
                    insertTradeHeap(trade_heap, row_arr, "Fill", row_arr[0], timestamp);
                    row_arr[0] = to_string(stof(row_arr[0]) + incrementor); // increment the order flow number for next row of the same order
                    timestamp = getCurrentTimestamp();
                    insertTradeHeap(trade_heap, buy_arr[0], "Fill", row_arr[0], timestamp);
                    row_arr[0] = to_string(stof(row_arr[0]) + incrementor);
                    buy_arr.erase(buy_arr.begin()); // remove the completed order from the order book
                    row_arr[4] = "0"; //order completed, quantity is zero
                    break;
                }
            }
        }
    }
}

//-------------------------------------MAIN FUNCTION FOR GENERATE THE ORDER CONSTRUCTION------------------------------------------------------------------------------------------------------------
void executeOrders(priority_queue<vector<string>, vector<vector<string>>, CompareVectors>& trade_heap ,unordered_map<string, vector<vector<string>>>& flower_fields, string flower) {
    // read the orders for given flower
    vector<vector<string>> flower_rows = flower_fields[flower];

    // order book for buy and sell side
    vector<vector<string>> sell_arr;
    vector<vector<string>> buy_arr;

    // read the each element of flower orders
    for (int i=0; i<flower_rows.size(); i++) {
        vector<string> row_arr = flower_rows[i]; // getting a row of a order
        string timestamp;
        // find orders are over
        if (row_arr.size() == 0) {
            break;
        }
        
        // making the order id
        string order_id = "ord";
        order_id.append(row_arr[0]);
        row_arr.push_back(order_id);

        if (stoi(row_arr[3]) == 1) { // buy order
            processBuyOrders(trade_heap, sell_arr, row_arr);
            if (row_arr[4] != "0") {
                addBuyArr(buy_arr, row_arr); // decending order
            }

        } else { // sell order
            processSellOrders(trade_heap, buy_arr, row_arr);
            if (row_arr[4] != "0") {
                addSellArr(sell_arr, row_arr); // ascending order
            }
        }
    }
}

//------------------------------------------ADD REJECTED ORDERS TO THE PRIORITY QUEUE--------------------------------------------------------------
void addRejectedOrders(priority_queue<vector<string>, vector<vector<string>>, CompareVectors>& trade_heap, unordered_map<string, vector<vector<string>>>& flower_fields) {
    vector<vector<string>> rejected_rows = flower_fields["Rejected"];
    for (int i=0; i<rejected_rows.size(); i++) {
        vector<string> row_arr = rejected_rows[i];
        string timestamp;
        if (row_arr.size() == 0) {
            break;
        }
        // making the order id
        string order_id = "ord";
        order_id.append(row_arr[0]);
        row_arr.push_back(order_id);

        timestamp = getCurrentTimestamp();
        insertTradeHeap(trade_heap, row_arr, "Reject", row_arr[0], timestamp);
    }
}

//--------------------------------------------PRINT A UNORDERED MAP CONTAINS VECTORS--------------------------------------------------------------
void readUnorderedMap(unordered_map<string, vector<vector<string>>> flower_fields) {
    for (auto it = flower_fields.begin(); it != flower_fields.end(); ++it) {
        cout << it->first << endl;
        vector<vector<string>> rows = it->second;
        for (vector<string> row : rows) {
            for (string field : row) {
                cout << field << " ";
            }
            cout << endl;
        }
        cout << endl;
    }
}


////////////////////////////////////////////// MAIN FUNCTION /////////////////////////////////////////////////////////////////
int main() {
    auto start = chrono::high_resolution_clock::now();

    // unordered map for store the orders in csv file
    unordered_map<string, vector<vector<string>>> flower_fields; 
    
    // priority queue for store the executed orders
    priority_queue<vector<string>, vector<vector<string>>, CompareVectors> trade_heap; 

    // read the csv file and store the orders in unordered map flower_fields
    readCsv("error_data.csv", flower_fields);

    // threads for each flower and rejected orders
    const int num_threads = 6;
    thread threads[num_threads];

    threads[0] = thread(executeOrders, ref(trade_heap), ref(flower_fields), "Rose");
    threads[1] = thread(executeOrders, ref(trade_heap), ref(flower_fields), "Lavender");
    threads[2] = thread(executeOrders, ref(trade_heap), ref(flower_fields), "Lotus");
    threads[3] = thread(executeOrders, ref(trade_heap), ref(flower_fields), "Tulip");
    threads[4] = thread(executeOrders, ref(trade_heap), ref(flower_fields), "Orchid");
    threads[5] = thread(addRejectedOrders, ref(trade_heap), ref(flower_fields));
    
    // wait until threads are completed
    for (int i = 0; i < num_threads; ++i) {
        threads[i].join();
    }

    auto end = chrono::high_resolution_clock::now();
    auto duration = chrono::duration_cast<chrono::microseconds>(end - start).count();
    cout << "Processing time: " << duration << " microseconds" << endl;

    //making the final csv file
    writeToCsv(trade_heap, "output.csv");
    
    // while (!trade_heap.empty()) {
    //     vector<string> row = trade_heap.top();
    //     trade_heap.pop();
    //     for (int j=0; j<row.size(); j++) {
    //         cout << row[j] << " ";
    //     }
    //     cout << endl;
    // }
    
    return 0;
}
