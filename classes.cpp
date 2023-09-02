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

class Order {
public:
    string customer_id;
    string instrument;
    string side;
    string quantity;
    string price;
    string order_id;
    string exec_status;
    string timestamp;
    string reason;
    string order_flow;

    Order(vector<string>& row) {
        order_flow = row[0];
        customer_id = row[1];
        instrument = row[2];
        side = row[3];
        quantity = row[4];
        price = row[5];
    };

    bool isBuy() {
        return this->side == "1";
    }

    void addToMap(unordered_map<string, vector<Order>>& order_map, string group) {
        order_map[group].push_back(*this);
    }
};

//---------------------------FUNCTION FOR PRIORITY QUEUE----------------------------------------------------
struct CompareVectors {
    bool operator()(const Order& a, const Order& b) const {
        return stof(a.order_flow) > stof(b.order_flow);  // greater-than comparison for min-heap behavior
    }
};


class OrderBook {
public:
    string instrument;
    vector<Order> buy_orders;
    vector<Order> sell_orders;

    OrderBook(string instrument) {
        this->instrument = instrument;
    }

    void addSellArr(const Order order_row) {
        float price = stof(order_row.price);

        auto insertion_pos = upper_bound(this->sell_orders.begin(), this->sell_orders.end(), price,
            [](float value, const Order ord) {
                return value < stof(ord.price); // Compare based on price
            }
        );

        this->sell_orders.insert(insertion_pos, order_row);
    }

    void addBuyArr(const Order order_row) {
        float price = stof(order_row.price);

        auto insertion_pos = upper_bound(this->buy_orders.begin(), this->buy_orders.end(), price,
            [](float value, const Order ord) {
                return value > stof(ord.price); // Compare based on price
            }
        );

        this->buy_orders.insert(insertion_pos, order_row);
    }

    void print() {
        cout << "Instrument: " << instrument << endl;
        cout << "Buy orders: " << endl;
        for (Order& order : buy_orders) {
            cout << order.order_id << " " << order.price << endl;
        }
        cout << "Sell orders: " << endl;
        for (Order& order : sell_orders) {
            cout << order.order_id << " " << order.price << endl;
        }
    }
};

class CSV {
public:
    // Constructor
    CSV(const string& filename) : filename(filename) {}

    void readCsv(unordered_map<string, vector<Order>>& order_map) {
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
                Order order(row);
                classifyOrders(order_map, order, error_code); // classify the orders into rejected and accepted according to the error code
            }
        }
    }

    void writeToCsv(priority_queue<Order, vector<Order>, CompareVectors> trade_heap) {
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

        // Write each Order object as a CSV row
        while (!trade_heap.empty()) {
            Order top_order = trade_heap.top();
            trade_heap.pop();
            outputFile 
                    << top_order.order_id << ","
                    << top_order.customer_id << ","
                    << top_order.instrument << ","
                    << top_order.side << ","
                    << top_order.exec_status << ","
                    << top_order.quantity << ","
                    << top_order.price << ","
                    << top_order.timestamp << ","
                    << top_order.reason << ","<< endl;
        }

        // Close the output file
        outputFile.close();

        cout << "CSV file created successfully." << endl;
    }

private:
    string filename;

    void classifyOrders(unordered_map<string, vector<Order>>& order_map, Order &order, int error_code) {
        string flowerName = order.instrument;
        if (error_code == 200) { // if everything OK
            order.reason = "";
            order.addToMap(order_map, flowerName);
        }
        else { // error occured
            if (error_code == 400) {
                order.reason = "Missing field";
            }
            else if (error_code == 401) {
                order.reason = "Invalid instrument";
            }
            else if (error_code == 402) {
                order.reason = "Invalid side";
            }
            else if (error_code == 403) {
                order.reason = "Invalid quantity";
            }
            else if (error_code == 404) {
                order.reason = "Invalid price";
            }
            order.exec_status = "Reject";
            order.addToMap(order_map, "Rejected");
        }
    }

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

class Trade {
public:
    priority_queue<Order, vector<Order>, CompareVectors> trade_heap;
    unordered_map<string, vector<Order>> order_map;

    void insertTradeHeap(Order order, string exec_status, string order_flow, string timestamp) {
        lock_guard<mutex> lock(mtx); // lock the thread until one thread is done
        int i = 0;
        if (exec_status == "Reject") i = 1; // because of the reason column order id shift to right side in rejected orders
        order.exec_status = exec_status;
        order.timestamp = timestamp;
        order.order_flow = order_flow;
        
        if (exec_status != "Reject") { 
            order.reason = ""; // reason
        }
        this->trade_heap.push(order);
        return;
    }

    void executeOrders(string flower) {
        // read the orders for given flower
        vector<Order> flower_rows = this->order_map[flower];

        // order book for buy and sell side
        OrderBook order_book(flower);

        // read the each element of flower orders
        for (int i=0; i<flower_rows.size(); i++) {
            Order order_row = flower_rows[i]; // getting a row of a order
            string timestamp;
            
            // making the order id
            string order_id = "ord";
            order_id.append(order_row.order_flow);
            order_row.order_id = order_id;

            if (order_row.isBuy()) { // buy order
                processBuyOrders(order_book, order_row);
                if (order_row.quantity != "0") {
                    order_book.addBuyArr(order_row); // decending order
                }
            
            } else { // sell order
                processSellOrders(order_book, order_row);
                if (order_row.quantity != "0") {
                    order_book.addSellArr(order_row); // ascending order
                }
            }
        }
    }

    void addRejectedOrders() {
        vector<Order> rejected_orders = this->order_map["Rejected"];
        for (int i=0; i<rejected_orders.size(); i++) {
            Order order_row = rejected_orders[i];
            string timestamp;
            
            // making the order id
            string order_id = "ord";
            order_id.append(order_row.order_flow);
            order_row.order_id = order_id;

            timestamp = getCurrentTimestamp();
            this->insertTradeHeap(order_row, "Reject", order_row.order_flow, timestamp);
        }
    }

private:
    void processBuyOrders(OrderBook& order_book, Order& order_row) {
        vector<Order>* sell_book = &order_book.sell_orders;
        string timestamp;
        float incrementor = 0.0001; // for make the correct ordering (if one order makes multiple trades, then the order flow number should be increment)
        
        if (sell_book->size() == 0) { // there are nothing to sell
            timestamp = getCurrentTimestamp();
            this->insertTradeHeap(order_row, "New", order_row.order_flow, timestamp);
        } else {
            if (stof((*sell_book)[0].price) > stof(order_row.price)) { // sell price is greater than to buy price
                timestamp = getCurrentTimestamp();
                this->insertTradeHeap(order_row, "New", order_row.order_flow, timestamp);
            } else {
                while (order_row.quantity != "0") { // quantity is not zero of the order
                    if (sell_book->size() == 0) { // there are nothing to sell
                        break;
                    }
                    if (stof((*sell_book)[0].price) > stof(order_row.price)) { // sell price is greater than to buy price
                        break;
                    }
                    int sell_quantity = stoi((*sell_book)[0].quantity);
                    int buy_quantity = stoi(order_row.quantity);
                    string row_price = order_row.price;
                    string sell_price = (*sell_book)[0].price;

                    // update the transaction price of the order according to the order book
                    order_row.price = sell_price;
                    
                    if (sell_quantity > buy_quantity) { // sell quantity is greater than buy quantity
                        (*sell_book)[0].quantity = to_string(buy_quantity); // set the transaction quantity
                        timestamp = getCurrentTimestamp();
                        this->insertTradeHeap(order_row, "Fill", order_row.order_flow, timestamp);
                        order_row.order_flow = to_string(stof(order_row.order_flow) + incrementor); // increment the order flow number for next row of the same order
                        order_row.quantity = "0"; // set the quantity to zero in order
                        timestamp = getCurrentTimestamp();
                        this->insertTradeHeap((*sell_book)[0], "PFill", order_row.order_flow, timestamp);
                        order_row.order_flow = to_string(stof(order_row.order_flow) + incrementor); // increment the order flow number for next row of the same order
                        (*sell_book)[0].quantity = to_string(sell_quantity - buy_quantity); // remaining quantity adds to the order book
                        break;

                    } else if (sell_quantity < buy_quantity) { // sell quantity is lesser than buy quantity
                        order_row.quantity = to_string(sell_quantity); // set the transaction quantity
                        timestamp = getCurrentTimestamp();
                        this->insertTradeHeap(order_row, "PFill",order_row.order_flow, timestamp);
                        order_row.order_flow = to_string(stof(order_row.order_flow) + incrementor); // increment the order flow number for next row of the same order
                        order_row.price = row_price; // set the price to the original price for remaining items
                        order_row.quantity = to_string(buy_quantity - sell_quantity); // remaining quantity adds to the order book 
                        timestamp = getCurrentTimestamp();
                        this->insertTradeHeap((*sell_book)[0], "Fill", order_row.order_flow, timestamp);
                        order_row.order_flow = to_string(stof(order_row.order_flow) + incrementor); // increment the order flow number for next row of the same order
                        sell_book->erase(sell_book->begin()); // remove the completed order from the order book

                    } else { // sell quantity is equal to buy quantity
                        timestamp = getCurrentTimestamp();
                        this->insertTradeHeap(order_row, "Fill", order_row.order_flow, timestamp);
                        order_row.order_flow = to_string(stof(order_row.order_flow) + incrementor); // increment the order flow number for next row of the same order
                        timestamp = getCurrentTimestamp();
                        this->insertTradeHeap((*sell_book)[0], "Fill", order_row.order_flow, timestamp);
                        order_row.order_flow = to_string(stof(order_row.order_flow) + incrementor);
                        sell_book->erase(sell_book->begin()); // remove the completed order from the order book
                        order_row.quantity = "0"; //order completed, quantity is zero
                        break;
                    }
                }
            } 
        }
    }

    void processSellOrders(OrderBook& order_book, Order& order_row) {
        vector<Order>* buy_book = &order_book.buy_orders;
        string timestamp;
        float incrementor = 0.0001; // for make the correct ordering (if one order makes multiple trades, then the order flow number should be increment)
        if (buy_book->size() == 0) { // there are nothing to buy
            timestamp = getCurrentTimestamp();
            this->insertTradeHeap(order_row, "New", order_row.order_flow, timestamp);
        } else {
            if ((*buy_book)[0].price < order_row.price) { // buy price is lesser than to sell price
                timestamp = getCurrentTimestamp();
                this->insertTradeHeap(order_row, "New", order_row.order_flow, timestamp);
            } else {
                while (order_row.quantity != "0") { // order quantity is not zero
                    
                    if (buy_book->size() == 0) { // there are nothing to buy
                        break;
                    }
                    if ((*buy_book)[0].price < order_row.price) { // buy price is lesser than to sell price
                        break;
                    }
                    int buy_quantity = stoi((*buy_book)[0].quantity);
                    int sell_quantity = stoi(order_row.quantity);
                    string row_price = order_row.price;
                    string buy_price = (*buy_book)[0].price;

                    // update the transaction price of the order according to the order book
                    order_row.price = buy_price;

                    if (buy_quantity > sell_quantity) { // buy quantity is greater than sell quantity
                        (*buy_book)[0].quantity = to_string(sell_quantity); // set the transaction quantity
                        timestamp = getCurrentTimestamp();
                        this->insertTradeHeap(order_row, "Fill", order_row.order_flow, timestamp);
                        order_row.order_flow = to_string(stof(order_row.order_flow) + incrementor); // increment the order flow number for next row of the same order
                        order_row.quantity = "0"; // set the quantity to zero in order, order completed
                        timestamp = getCurrentTimestamp();
                        this->insertTradeHeap((*buy_book)[0], "PFill", order_row.order_flow, timestamp);
                        order_row.order_flow = to_string(stof(order_row.order_flow) + incrementor); // increment the order flow number for next row of the same order
                        (*buy_book)[0].quantity = to_string(buy_quantity - sell_quantity); // remaining quantity adds to the order book
                        break;

                    } else if (buy_quantity < sell_quantity) { // buy quantity is lesser than sell quantity
                        order_row.quantity = to_string(buy_quantity); // set the transaction quantity
                        timestamp = getCurrentTimestamp();
                        this->insertTradeHeap(order_row, "PFill", order_row.order_flow, timestamp);
                        order_row.order_flow = to_string(stof(order_row.order_flow) + incrementor); // increment the order flow number for next row of the same order
                        order_row.price = row_price; // set the price to the original price for remaining items
                        order_row.quantity = to_string(sell_quantity - buy_quantity); // remaining quantity adds to the order book
                        timestamp = getCurrentTimestamp();
                        this->insertTradeHeap((*buy_book)[0], "Fill", order_row.order_flow, timestamp);
                        order_row.order_flow = to_string(stof(order_row.order_flow) + incrementor); // increment the order flow number for next row of the same order
                        buy_book->erase(buy_book->begin()); // remove the completed order from the order book

                    } else { // buy quantity is equal to sell quantity
                        timestamp = getCurrentTimestamp();
                        this->insertTradeHeap(order_row, "Fill", order_row.order_flow, timestamp);
                        order_row.order_flow = to_string(stof(order_row.order_flow) + incrementor); // increment the order flow number for next row of the same order
                        timestamp = getCurrentTimestamp();
                        this->insertTradeHeap((*buy_book)[0], "Fill", order_row.order_flow, timestamp);
                        order_row.order_flow = to_string(stof(order_row.order_flow) + incrementor);
                        buy_book->erase(buy_book->begin()); // remove the completed order from the order book
                        order_row.quantity = "0"; //order completed, quantity is zero
                        break;
                    }
                }
            }
        }
    }

};



//-------------------------------------INSERT NEW ROW IN PRIORITY QUEUE-----------------------------------------


//-------------------------------------ALGORITHM FOR THE BUY ORDERS---------------------------------------------------


//-------------------------------------MAIN FUNCTION FOR GENERATE THE ORDER CONSTRUCTION------------------------------------------------------------------------------------------------------------


//--------------------------------PRINT A UNORDERED MAP CONTAINS VECTORS---------------------------------------------------
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
    // unordered_map<string, vector<vector<string>>> flower_fields;

    Trade trade;

    //unordered_map<string, vector<Order>> order_map; 
    
    // priority queue for store the executed orders
    //priority_queue<Order, vector<Order>, CompareVectors> trade_heap; 

    // read the csv file and store the orders in unordered map flower_fields
    CSV read_file("ex8.csv");
    read_file.readCsv(trade.order_map);

    // threads for each flower and rejected orders
    const int num_threads = 6;
    thread threads[num_threads];

    threads[0] = thread(&Trade::executeOrders, &trade, "Rose");
    threads[1] = thread(&Trade::executeOrders, &trade, "Lavender");
    threads[2] = thread(&Trade::executeOrders, &trade, "Lotus");
    threads[3] = thread(&Trade::executeOrders, &trade, "Tulip");
    threads[4] = thread(&Trade::executeOrders, &trade, "Orchid");
    threads[5] = thread(&Trade::addRejectedOrders, &trade);
    
    // wait until threads are completed
    for (int i = 0; i < num_threads; ++i) {
        threads[i].join();
    }

    auto end = chrono::high_resolution_clock::now();
    auto duration = chrono::duration_cast<chrono::microseconds>(end - start).count();
    cout << "Processing time: " << duration << " microseconds" << endl;

    //making the final csv file
    CSV write_file("output.csv");
    write_file.writeToCsv(trade.trade_heap);
    
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
