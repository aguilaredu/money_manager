// ==UserScript==
// @name         New Userscript
// @namespace    http://tampermonkey.net/
// @version      2023-12-30
// @description  try to take over the world!
// @author       You
// @match        https://*.sucursalelectronica.com/*
// @icon         https://www.google.com/s2/favicons?sz=64&domain=tampermonkey.net
// @grant        none
// @require      file:///C:/Users/user/Desktop/scraper.js
// ==/UserScript==

// Check if an element has display none
function isElementVisible(elem) {
    if (!elem) return false;
    if (elem.style.display === 'none' || window.getComputedStyle(elem).display === 'none') {
        return false;
    }

    while (elem) {
        if (elem.style.display === 'none' || window.getComputedStyle(elem).display === 'none') {
            return false;
        }
        elem = elem.parentElement;
    }

    return true;
}

// Create function to get my HTML table by ID. Only the active table can be retrieved
function getTableDataById(tableIdList) {
    for (let id of tableIdList) {
        var element = document.getElementById(id);

        if(element)

        if (element) {
            if (isElementVisible(element)){
                return element;
            }
        }
    } 
    return null
}

function trimNewlinesAndTabs(str) {
    return str.replace(/^[\n\t]+|[\n\t]+$/g, '');
}

function arrayToCSV(array) {
    // Function to escape double quotes and enclose the field in double quotes if necessary
    const escapeField = (field) => {
        if (field.includes(',') || field.includes('\n') || field.includes('"')) {
            return `"${field.replace(/"/g, '""')}"`;
        }
        return field;
    };

    // Convert the array of arrays to a CSV string
    return array.map(row => 
        row.map(escapeField).join(',')
    ).join('\n');
}

function tableToList(table) {
    var result = [];

    // Iterate over each row in the table
    for (var i = 0, row; row = table.rows[i]; i++) {
        var rowList = [];

        // Iterate over each cell in the row
        for (var j = 0, col; col = row.cells[j]; j++) {
            // Push the text content of the cell to the row array
            rowList.push(trimNewlinesAndTabs(col.textContent) || trimNewlinesAndTabs(col.innerText));
        }

        // Push the row array to the result array
        result.push(rowList);
    }

    return result;
}

function downloadCSV(array, filename) {
    // Convert array to CSV
    var csvContent = arrayToCSV(array); // Use the arrayToCSV function from the previous example
    console.log(csvContent)
    // Create a Blob from the CSV string
    var BOM = "\uFEFF";
    var blob = new Blob([BOM + csvContent], { type: 'text/csv;charset=utf-8;' });

    // Create a link and set the URL and download attributes
    var link = document.createElement("a");
    if (link.download !== undefined) { // Feature detection for download attribute
        var url = URL.createObjectURL(blob);
        link.setAttribute("href", url);
        link.setAttribute("download", filename);

        // Append link to the body, trigger click, and remove it
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);

        // Clean up by revoking the Blob URL
        URL.revokeObjectURL(url);
    }
}


// Create a new button element
var myButton = document.createElement('button');
myButton.innerHTML = 'Get data';
myButton.id = 'getDataBtn';

// Custom styling for the button
myButton.style.position = 'fixed';
myButton.style.top = '10px';
myButton.style.right = '10px';
myButton.style.zIndex = '1000';
myButton.style.border = '1px solid #000'; // Black border
myButton.style.backgroundColor = '#808080'; // Gray background
myButton.style.borderRadius = '2px'; // Rounded corners
myButton.style.padding = '5px 10px'; // Padding inside the button
myButton.style.cursor = 'pointer'; // Cursor pointer on hover

// Append the button to the body
document.body.appendChild(myButton);

function getData(){
    tableIds = ['creditCardRecentMovementsTable', 'creditCardStateTRX', 'transactionTable', 'transactionTable1'];

    // Get the active table data 
    var tableElement = getTableDataById(tableIds);

    // Convert it to a list
    var tableData = tableToList(tableElement)

    // Convert to CSV
    downloadCSV(tableData, "data.csv")

}

// Add event listener to the button
myButton.addEventListener('click', function() {
    // Clear any previous data if necessary

    // Then fetch new data
    getData();
});


