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


////////////////////////////////////////////// ORDER CLASS //////////////////////////////////////////////////////
class Order {
public:
    string customer_id;
    string instrument;
    string side;
    string quantity;
    string price;
    string order_id;
    string exec_status; // New, Fill, PFill, Reject
    string timestamp;
    string reason; // reason for reject
    string order_flow; // use for make the correct ordering

    // Constructor
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

    //----------------------------------- ADD A ORDER TO THE UNORDERED MAP----------------------------------
    void addToMap(unordered_map<string, vector<Order>>& order_map, string group) {
        order_map[group].push_back(*this);
    }
};
////////////////////////////////////////////////////////////////////////////////////////////////////////////


//---------------------------FUNCTION FOR PRIORITY QUEUE----------------------------------------------------
struct CompareVectors {
    bool operator()(const Order& a, const Order& b) const {
        return stof(a.order_flow) > stof(b.order_flow);  // greater-than comparison for min-heap behavior
    }
};

//------------------------------MAKING THE TIME-STAMP-------------------------------------------------------
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



///////////////////////////////////////// ORDER BOOK CLASS //////////////////////////////////////////////////
class OrderBook {
public:
    string instrument;
    vector<Order> buy_orders; // decending order
    vector<Order> sell_orders; // ascending order

    // Constructor
    OrderBook(string instrument) {
        this->instrument = instrument;
    }

    //-------------------------------- ADD A SELL ORDER TO THE ORDER BOOK----------------------------------
    void addSellArr(const Order order) {
        float price = stof(order.price);

        auto insertion_pos = upper_bound(this->sell_orders.begin(), this->sell_orders.end(), price,
            [](float value, const Order ord) {
                return value < stof(ord.price); // Compare based on price
            }
        );

        this->sell_orders.insert(insertion_pos, order);
    }

    //-------------------------------- ADD A BUY ORDER TO THE ORDER BOOK-----------------------------------
    void addBuyArr(const Order order) {
        float price = stof(order.price);

        auto insertion_pos = upper_bound(this->buy_orders.begin(), this->buy_orders.end(), price,
            [](float value, const Order ord) {
                return value > stof(ord.price); // Compare based on price
            }
        );

        this->buy_orders.insert(insertion_pos, order);
    }

    // Print the order book
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



///////////////////////////////////////////// CSV CLASS /////////////////////////////////////////////////////////
class CSV {
public:
    // Constructor
    CSV(const string& filename) : filename(filename) {}

    //----------------------------------- READ THE CSV FILE-----------------------------------------------------
    void readCsv(unordered_map<string, vector<Order>>& order_map) {
        ifstream inputFile(filename);
        if (!inputFile.is_open()) {
            cerr << "Error opening file." << endl;
            return;
        }

        string line;
        int count = 0;
        getline(inputFile, line); // Skip the first line (header)
        while (getline(inputFile, line)) {
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
                // classify the orders into rejected and accepted according to the error code
                classifyOrders(order_map, order, error_code);
            }
        }

        inputFile.close(); // Close the file
    }

    //----------------------------------- WRITE TO THE CSV FILE-----------------------------------------------------
    void writeToCsv(priority_queue<Order, vector<Order>, CompareVectors> trade_queue) {
        ofstream outputFile(filename);
        if (!outputFile.is_open()) {
            cerr << "Error opening output file." << endl;
            return;
        }

        // adds the header row first
        vector<string> trade_arr = {"Order ID", "Client Order ID", "Instrument", "Side", 
                                        "Exec Status", "Quantity", "Price", "Transaction Time", "Reason"}; // header
        for (size_t i = 0; i < trade_arr.size(); ++i) {
            outputFile << trade_arr[i];
            if (i < trade_arr.size() - 1) {
                outputFile << ",";
            }
        }
        outputFile << endl;

        // Write each Order object as a CSV row
        while (!trade_queue.empty()) {
            Order top_order = trade_queue.top();
            trade_queue.pop();
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

    //----------------------------------- CLASSIFY THE ORDERS AND ADDS THE REASON -----------------------------------------------------
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

    //----------------------------------- CHECK THE REQUIREMENTS OF A ORDER -----------------------------------------------------
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




/////////////////////////////////////////////////// TRADE CLASS ////////////////////////////////////////////////////////////////
class Trade {
public:
    priority_queue<Order, vector<Order>, CompareVectors> trade_queue;
    unordered_map<string, vector<Order>> order_map;

    //----------------------------------- INSERT A ORDER TO THE PRIORITY QUEUE----------------------------------
    void insertTradeHeap(Order order, string exec_status, string order_flow, string timestamp) {
        lock_guard<mutex> lock(mtx); // lock the thread until one thread is done
        order.exec_status = exec_status;
        order.timestamp = timestamp;
        order.order_flow = order_flow;
        
        // rejected orders already has a reason
        if (exec_status != "Reject") { 
            order.reason = ""; // reason
        }
        this->trade_queue.push(order);
        return;
    }

    //----------------------------------- EXECUTE THE ORDERS FOR A GIVEN FLOWER----------------------------------
    void executeOrders(string flower) {
        // read the orders for given flower
        vector<Order> flower_rows = this->order_map[flower];

        // order book for buy and sell side
        OrderBook order_book(flower);

        // read the each element of flower orders
        for (int i=0; i<flower_rows.size(); i++) {
            Order order = flower_rows[i]; // getting a row of a order
            string timestamp;
            
            // making the order id
            string order_id = "ord";
            order_id.append(order.order_flow);
            order.order_id = order_id;

            if (order.isBuy()) { // buy order
                processBuyOrders(order_book, order);
                if (order.quantity != "0") {
                    order_book.addBuyArr(order); // decending order
                }
            
            } else { // sell order
                processSellOrders(order_book, order);
                if (order.quantity != "0") {
                    order_book.addSellArr(order); // ascending order
                }
            }
        }
    }

    //----------------------------------- ADD THE REJECTED ORDERS TO THE PRIORITY QUEUE----------------------------------
    void addRejectedOrders() {
        vector<Order> rejected_orders = this->order_map["Rejected"];
        for (int i=0; i<rejected_orders.size(); i++) {
            Order order = rejected_orders[i];
            string timestamp;
            
            // making the order id
            string order_id = "ord";
            order_id.append(order.order_flow);
            order.order_id = order_id;

            timestamp = getCurrentTimestamp();
            this->insertTradeHeap(order, "Reject", order.order_flow, timestamp);
        }
    }

private:
    //------------------------------------------------------ PROCESS THE BUY ORDERS---------------------------------------------------------
    void processBuyOrders(OrderBook& order_book, Order& order) {
        vector<Order>* sell_book = &order_book.sell_orders;
        string timestamp;
        float incrementor = 0.0001; // for make the correct ordering (if one order makes multiple trades, then the order flow number
                                        // should be increment) 0.0001 is enough bcoz maximum quantity is 1000.
        
        if (sell_book->size() == 0) { // there are nothing to sell
            timestamp = getCurrentTimestamp();
            this->insertTradeHeap(order, "New", order.order_flow, timestamp);
        } else {
            if (stof((*sell_book)[0].price) > stof(order.price)) { // sell price is greater than to buy price
                timestamp = getCurrentTimestamp();
                this->insertTradeHeap(order, "New", order.order_flow, timestamp);
            } else {
                while (order.quantity != "0") { // quantity is not zero of the order
                    if (sell_book->size() == 0) { // there are nothing to sell
                        break;
                    }
                    if (stof((*sell_book)[0].price) > stof(order.price)) { // sell price is greater than to buy price
                        break;
                    }
                    int sell_quantity = stoi((*sell_book)[0].quantity);
                    int buy_quantity = stoi(order.quantity);
                    string row_price = order.price;
                    string sell_price = (*sell_book)[0].price;

                    // update the transaction price of the order according to the order book
                    order.price = sell_price;
                    
                    if (sell_quantity > buy_quantity) { // sell quantity is greater than buy quantity
                        (*sell_book)[0].quantity = to_string(buy_quantity); // set the transaction quantity
                        timestamp = getCurrentTimestamp();
                        this->insertTradeHeap(order, "Fill", order.order_flow, timestamp);
                        order.order_flow = to_string(stof(order.order_flow) + incrementor); // increment the order flow number for next row of the same order
                        order.quantity = "0"; // set the quantity to zero in order
                        timestamp = getCurrentTimestamp();
                        this->insertTradeHeap((*sell_book)[0], "PFill", order.order_flow, timestamp);
                        order.order_flow = to_string(stof(order.order_flow) + incrementor); // increment the order flow number for next row of the same order
                        (*sell_book)[0].quantity = to_string(sell_quantity - buy_quantity); // remaining quantity adds to the order book
                        break;

                    } else if (sell_quantity < buy_quantity) { // sell quantity is lesser than buy quantity
                        order.quantity = to_string(sell_quantity); // set the transaction quantity
                        timestamp = getCurrentTimestamp();
                        this->insertTradeHeap(order, "PFill",order.order_flow, timestamp);
                        order.order_flow = to_string(stof(order.order_flow) + incrementor); // increment the order flow number for next row of the same order
                        order.price = row_price; // set the price to the original price for remaining items
                        order.quantity = to_string(buy_quantity - sell_quantity); // remaining quantity adds to the order book 
                        timestamp = getCurrentTimestamp();
                        this->insertTradeHeap((*sell_book)[0], "Fill", order.order_flow, timestamp);
                        order.order_flow = to_string(stof(order.order_flow) + incrementor); // increment the order flow number for next row of the same order
                        sell_book->erase(sell_book->begin()); // remove the completed order from the order book

                    } else { // sell quantity is equal to buy quantity
                        timestamp = getCurrentTimestamp();
                        this->insertTradeHeap(order, "Fill", order.order_flow, timestamp);
                        order.order_flow = to_string(stof(order.order_flow) + incrementor); // increment the order flow number for next row of the same order
                        timestamp = getCurrentTimestamp();
                        this->insertTradeHeap((*sell_book)[0], "Fill", order.order_flow, timestamp);
                        order.order_flow = to_string(stof(order.order_flow) + incrementor);
                        sell_book->erase(sell_book->begin()); // remove the completed order from the order book
                        order.quantity = "0"; //order completed, quantity is zero
                        break;
                    }
                }
            } 
        }
    }

    //------------------------------------------------------ PROCESS THE SELL ORDERS---------------------------------------------------------
    void processSellOrders(OrderBook& order_book, Order& order) {
        vector<Order>* buy_book = &order_book.buy_orders;
        string timestamp;
        float incrementor = 0.0001; // for make the correct ordering (if one order makes multiple trades, then the order flow number 
                                        // should be increment) 0.0001 is enough bcoz maximum quantity is 1000.
        if (buy_book->size() == 0) { // there are nothing to buy
            timestamp = getCurrentTimestamp();
            this->insertTradeHeap(order, "New", order.order_flow, timestamp);
        } else {
            if ((*buy_book)[0].price < order.price) { // buy price is lesser than to sell price
                timestamp = getCurrentTimestamp();
                this->insertTradeHeap(order, "New", order.order_flow, timestamp);
            } else {
                while (order.quantity != "0") { // order quantity is not zero
                    
                    if (buy_book->size() == 0) { // there are nothing to buy
                        break;
                    }
                    if ((*buy_book)[0].price < order.price) { // buy price is lesser than to sell price
                        break;
                    }
                    int buy_quantity = stoi((*buy_book)[0].quantity);
                    int sell_quantity = stoi(order.quantity);
                    string row_price = order.price;
                    string buy_price = (*buy_book)[0].price;

                    // update the transaction price of the order according to the order book
                    order.price = buy_price;

                    if (buy_quantity > sell_quantity) { // buy quantity is greater than sell quantity
                        (*buy_book)[0].quantity = to_string(sell_quantity); // set the transaction quantity
                        timestamp = getCurrentTimestamp();
                        this->insertTradeHeap(order, "Fill", order.order_flow, timestamp);
                        order.order_flow = to_string(stof(order.order_flow) + incrementor); // increment the order flow number for next row of the same order
                        order.quantity = "0"; // set the quantity to zero in order, order completed
                        timestamp = getCurrentTimestamp();
                        this->insertTradeHeap((*buy_book)[0], "PFill", order.order_flow, timestamp);
                        order.order_flow = to_string(stof(order.order_flow) + incrementor); // increment the order flow number for next row of the same order
                        (*buy_book)[0].quantity = to_string(buy_quantity - sell_quantity); // remaining quantity adds to the order book
                        break;

                    } else if (buy_quantity < sell_quantity) { // buy quantity is lesser than sell quantity
                        order.quantity = to_string(buy_quantity); // set the transaction quantity
                        timestamp = getCurrentTimestamp();
                        this->insertTradeHeap(order, "PFill", order.order_flow, timestamp);
                        order.order_flow = to_string(stof(order.order_flow) + incrementor); // increment the order flow number for next row of the same order
                        order.price = row_price; // set the price to the original price for remaining items
                        order.quantity = to_string(sell_quantity - buy_quantity); // remaining quantity adds to the order book
                        timestamp = getCurrentTimestamp();
                        this->insertTradeHeap((*buy_book)[0], "Fill", order.order_flow, timestamp);
                        order.order_flow = to_string(stof(order.order_flow) + incrementor); // increment the order flow number for next row of the same order
                        buy_book->erase(buy_book->begin()); // remove the completed order from the order book

                    } else { // buy quantity is equal to sell quantity
                        timestamp = getCurrentTimestamp();
                        this->insertTradeHeap(order, "Fill", order.order_flow, timestamp);
                        order.order_flow = to_string(stof(order.order_flow) + incrementor); // increment the order flow number for next row of the same order
                        timestamp = getCurrentTimestamp();
                        this->insertTradeHeap((*buy_book)[0], "Fill", order.order_flow, timestamp);
                        order.order_flow = to_string(stof(order.order_flow) + incrementor);
                        buy_book->erase(buy_book->begin()); // remove the completed order from the order book
                        order.quantity = "0"; //order completed, quantity is zero
                        break;
                    }
                }
            }
        }
    }

};




////////////////////////////////////////////// MAIN FUNCTION /////////////////////////////////////////////////////////////////
int main() {

    Trade trade;

    // read the csv file and store the orders in unordered map flower_fields
    CSV read_file("./examples/random_data.csv");
    read_file.readCsv(trade.order_map);


    // threads for each flower and rejected orders
    const int num_threads = 6;
    thread threads[num_threads];

    string flowers[] = {"Rose", "Lavender", "Lotus", "Tulip", "Orchid"};
    for (int i = 0; i < num_threads-1; i++) {
        threads[i] = thread(&Trade::executeOrders, &trade, flowers[i]);
    }
    threads[5] = thread(&Trade::addRejectedOrders, &trade);
    
    // wait until threads are completed
    for (int i = 0; i < num_threads; ++i) {
        threads[i].join();
    }

    // making the final csv file
    CSV write_file("execution_rep.csv");
    write_file.writeToCsv(trade.trade_queue);
    
    return 0;
}
