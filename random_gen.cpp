#include <iostream>
#include <fstream>
#include <string>
#include <random>

// Generate a random integer within a specified range
int getRandomInt(int min, int max) {
    static std::random_device rd;
    static std::mt19937 gen(rd());
    std::uniform_int_distribution<int> dis(min, max);
    return dis(gen);
}

// Generate a random quantity that is a multiple of 10
int getRandomQuantity(int min, int max) {
    int quantity = getRandomInt(min, max);
    return quantity - (quantity % 10); // Adjust quantity to be a multiple of 10
}

int main() {
    const int numRows = 100000; // Change this to the desired number of rows
    const std::string instruments[] = {"Rose", "Lavender", "Tulip", "Orchid", "Lotus"};
    const int numInstruments = sizeof(instruments) / sizeof(instruments[0]);
    const int sides[] = {1, 2};
    const int numSides = sizeof(sides) / sizeof(sides[0]);
    const int minQuantity = 10;
    const int maxQuantity = 1000;
    const int minPrice = 1;
    const int maxPrice = 100;

    std::ofstream outputFile("random_data.csv");
    if (!outputFile.is_open()) {
        std::cerr << "Error opening output file." << std::endl;
        return 1;
    }

    outputFile << "Client Order ID,Instrument,Side,Quantity,Price" << std::endl;

    for (int i = 0; i < numRows; ++i) {
        std::string orderID = "aa" + std::to_string(10 + i);
        std::string instrument = instruments[getRandomInt(0, numInstruments - 1)];
        int side = sides[getRandomInt(0, numSides - 1)];
        int quantity = getRandomQuantity(minQuantity, maxQuantity);
        int price = getRandomInt(minPrice, maxPrice);

        outputFile << orderID << "," << instrument << "," << side << "," << quantity << "," << price << std::endl;
    }

    outputFile.close();
    std::cout << "CSV file created successfully." << std::endl;

    return 0;
}
