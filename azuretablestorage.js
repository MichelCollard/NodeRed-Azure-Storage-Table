module.exports = function (RED) {

    var Client = require('azure-storage');
    var globaltable = null;
    var clientTableService = null;
    var clientAccountName = "";
    var clientAccountKey = "";
    var node = null;
    var nodeConfig = null;

    var statusEnum = {
        disconnected: { color: "red", text: "Disconnected" },
        sending: { color: "green", text: "Sending" },
        sent: { color: "blue", text: "Sent message" },
        error: { color: "grey", text: "Error" }
    };

    var setStatus = function (status) {
        node.status({ fill: status.color, shape: "dot", text: status.text });
    }

    var senddata = function (msg) {
            var entityClass = msg.payload;
			node.log('Saving data into Azure Table Storage :\n   data: ' + entityClass.partitionKey + " - " + entityClass.rowKey + " - " + entityClass.data + " - " + entityClass.tableName);
            // Create a message and send it to the Azure Table Storage
            var entGen = Client.TableUtilities.entityGenerator;
            node.log('creating entity...');
            var entity = {};
            var jdata = null;

            if (typeof (entityClass.data) != "string") {
                jdata = entityClass.data;
            } else {
                jdata = JSON.parse(entityClass.data);
            }

            for(var key in jdata) {
                    entity[key] = entGen.String(jdata[key]);      
                };
            node.log(JSON.stringify(entity));
            entity["PartitionKey"] = entGen.String(entityClass.partitionKey);
            entity["RowKey"]= entGen.String(entityClass.rowKey);

            node.log(JSON.stringify(entity));
            node.log('entity created successfully');
            clientTableService.insertEntity(entityClass.tableName, entity, function(err, result, response) {
                node.log('trying to insert');
                if (err) {
                    node.error('Error while trying to save data:' + err.toString());
                    setStatus(statusEnum.error);
                } else {
                    node.log('data saved.');
                    setStatus(statusEnum.sent);
					msg.payload = 'data saved.';
                    node.send(msg);
                }
            });
        };

    var readdata = function (msg, table, pkey, rkey) {
            var entityClass = msg.payload;
			node.log('Reading data from Azure Table Storage :\n   data: ' + entityClass.partitionKey + " - " + entityClass.rowKey);
            clientTableService.retrieveEntity(entityClass.tableName, entityClass.partitionKey, entityClass.rowKey, function(err, result, response) {
                if (err) {
                    node.error('Error while trying to read data:' + err.toString());
                    setStatus(statusEnum.error);
                } else {
                    node.log(result.data._);
                    setStatus(statusEnum.sent);
                    node.send(result.data._);
                }
            });
         }

    var deleteTable = function (table) {
        node.log("Deleting table");
        clientTableService.deleteTable(table, function (err) {
             if (err) {
                node.error('Error while trying to delete table:' + err.toString());
                setStatus(statusEnum.error);
            } else {
                node.log('table deleted');
                setStatus(statusEnum.sent);
                msg.payload = 'table deleted';
                node.send(msg);
            }   
        });
    }

    var uptadeEntity = function (msg) {
            var entityClass = msg.payload;
			node.log('updating entity');
            var entity = {
                PartitionKey: entGen.String(entityClass.partitionKey),
                RowKey: entGen.String(entityClass.rowKey),
                data: entGen.String(entityClass.data),
            };
            clientTableService.insertOrReplaceEntity(entityClass.tableName, entity, function(err, result, response){
                if (err) {
                    node.error('Error while trying to update entity:' + err.toString());
                    setStatus(statusEnum.error);
                } else {
                    node.log('entity updated');
                    setStatus(statusEnum.sent);
                    msg.payload = 'entity updated';
					node.send(msg);
                } 
            });
         }

    var deleteEntity = function (msg) {
            var entityClass = msg.payload;
			node.log('deleting entity');
            var entity = {
                PartitionKey: entGen.String(entityClass.partitionKey),
                RowKey: entGen.String(entityClass.rowKey),
                data: entGen.String(entityClass.data),
            };
            clientTableService.deleteEntity(entityClass.tableName, entity, function(err, result, response){
                if (err) {
                    node.error('Error while trying to delete entity:' + err.toString());
                    setStatus(statusEnum.error);
                } else {
                    node.log('entity deleted');
                    setStatus(statusEnum.sent);
                    msg.payload = 'entity deleted';
					node.send(msg);
                } 
            }); 
         }


    var queryEntity = function (table, fromcolumn, where, selectdata) {
        var entityClass = msg.payload;
		node.log('query entity');
        var query = new Client.TableQuery()
            .top(1)
            .where(entityClass.fromcolumn + ' eq ?', entityClass.where);
        clientTableService.queryEntities(entityClass.tableName, query, null, function(err, result, response){
            if (err) {
                node.error('Error while trying to query entity:' + err.toString());
                setStatus(statusEnum.error);
            } else {
                //node.log(JSON.stringify(result.entries.data));
                //setStatus(statusEnum.sent);
                //node.send(result.entries.data._);
            } 
        });
    }

    var disconnectFrom = function () { 
         if (clientTableService) { 
             node.log('Disconnecting from Azure'); 
             clientTableService.removeAllListeners(); 
             clientTableService = null;
             setStatus(statusEnum.disconnected); 
         } 
     } 


    function createTable(msg, callback) {
        var entityClass = msg.payload;
		node.log('Creating a table if not exists');
        var tableService = Client.createTableService(clientAccountName, clientAccountKey);
        clientTableService = tableService;
        tableService.createTableIfNotExists(entityClass.tableName, function(error, result, response) {
        if (!error) {
                // result contains true if created; false if already exists
                globaltable = entityClass.tableName;
                callback(msg);
         }
         else {
             node.error(error);
         }
        });
    }

    // Main function called by Node-RED    
    function AzureTableStorage(config) {
        // Store node for further use
        node = this;
        nodeConfig = config;

        // Create the Node-RED node
        RED.nodes.createNode(this, config);
        clientAccountName = node.credentials.accountname
        clientAccountKey = node.credentials.key;

        this.on('input', function (msg) {

            node.log('Received the input: ' + msg.payload.tableName);
            var action = msg.payload.action;
            // Sending data to Azure Table Storage
            setStatus(statusEnum.sending);
            switch (action) {
                case "I":
                    node.log('Trying to insert entity');
                    createTable(msg, senddata);
                    break;
                case "R":
                    node.log('Trying to read entity');
                    createTable(msg, readdata);
                    break;
                case "DT":
                    node.log('Trying to delete table');
                    deleteTable(msg.tableName);
                    break;
                case "Q":
                    //node.log('Trying to query data');
                    //queryEntity(messageJSON.tableName, messageJSON.fromColumn, messageJSON.where, messageJSON.selectData);
                    break;
                case "U":
                    node.log('trying to update entity');
                    createTable(msg, uptadeEntity);
                    break;
                case "D":
                    node.log('trying to delete entity');
                    createTable(msg, deleteEntity);
                    break;
                default:
                    node.log('action was not detected');
                    node.error('action was not detected');
                    setStatus(statusEnum.error);
                    break;
            }    
        });

        this.on('close', function () {
            disconnectFrom(this);
        });
    }

    // Registration of the node into Node-RED
    RED.nodes.registerType("Table Storage", AzureTableStorage, {
        credentials: {
            accountname: { type: "text" },
            key: { type: "text" },
        },
        defaults: {
            name: { value: "Azure Table Storage" },
        }
    });


    // Helper function to print results in the console
    function printResultFor(op) {
        return function printResult(err, res) {
            if (err) node.error(op + ' error: ' + err.toString());
            if (res) node.log(op + ' status: ' + res.constructor.name);
        };
    }
}