var _ = ( function( ){
	
	var defaultModules = function( ){ };
	defaultModules.prototype.merge = function( otherModules, forceOverrideModules ){
		for( var moduleName in otherModules ){
			if( !this[ moduleName ] || forceOverrideModules ){
				this[ moduleName ] = otherModules[ moduleName ];
			}
		}
		return this;
	}

	defaultModules = new defaultModules( );

	defaultModules.dbox = require( "dbox" );
	defaultModules.async = require( "async" );
	defaultModules.http = require( "http" );
	defaultModules.url = require( "url" );
	defaultModules.uuid = require( "node-uuid" );
	defaultModules.crypto = require( "crypto" );
	defaultModules.mongo = require( "mongodb" );
	defaultModules.process = require( "child_process" );
	defaultModules.fs = require( "fs" );

	return defaultModules;
} )( );

//Local constants
const LINK_TYPE = "link command";

/*
	Get the type of platform this node is running.
*/
_.platformType = process.platform;

//Reference link commands.
function constructLinkCommands( linkCommands ){
	if( !linkCommands.hasLinkedCommands ){
		linkCommands = linkCommands || {
			authorize: authorize,
			add: add,
			remove: remove,
			check: check,
			loadDb: loadDb,
			link: link				
		};

		for( var command in linkCommands ){
			linkCommands[ command ].type = linkCommands[ command ].type || LINK_TYPE;
		}

		//Circularized intelligently.
		//This prevents memory leaks.
		linkCommands.getLinkedCommands = linkCommands.getLinkedCommands || function( ){
			return linkCommands;
		}

		linkCommands.hasLinkedCommands = true;
	}
	
	return linkCommands;
};



function listProcedures( linkCommands, procedure ){
	try{
		var chainFunction;

		//Mask the link commands as delegates
		//We have to retain the previous.
		linkCommands.delegates = linkCommands.delegates || {};

		//This will either assign or overrides.
		for( var command in linkCommands ){
			if( linkCommands[ command ].type == LINK_TYPE ){
				//First, prepare the originals we don't want them to be overriden.
				var delegates = {};
				for( var _command in linkCommands.delegates ){
					if( command != _command ){
						delegates[ _command ] = linkCommands.delegates[ _command ];
					}
				}
				linkCommands.delegates[ command ] = function( ){
					clearTimeout( chainFunction.run );
					//When this function is called, restore the other functions to originals.
					for( var _command in delegates ){
						linkCommands.delegates[ _command ] = delegates[ _command ];
					}
					//Mark the procedure with an ID so that it will not be pushed again.
					if( !procedure.id ){
						procedure.id = _.crypto.createHash( "md5" )
							.update( _.uuid.v4( ), "utf8" )
							.digest( "hex" ).toString( );
						( linkCommands.procedures || linkCommands.procedures = [ ] )
							.push( procedure );	
					}
					//Now a link can be linked once ( by linking we are also calling it )
					//When a link is linked, it should restore the state of other links.
					linkCommands[ command ].apply( linkCommands, arguments );
				}
			}
		}
		
		//I am thinking of timed calls.
		chainFunction =  function( config ){
			//Push the configuration.
			( linkCommands.configs || linkCommands.configs = [ ] ).push( config );
			
			//If a next function is not called within 100 milliseconds ( please lower if possible )
			//Execute this run function. 
			setTimeout( chainFunction.run, 100 );
		};

		//Merge the delegates to the chain function.
		for( var command in linkCommands.delegates ){
			chainFunction[ command ] = linkCommands.delegates[ command ];
		}

		//The supplied parameter is the last config for the last procedure.
		chainFunction.run = function( config ){
			if( linkCommands.procedures.length - 1 == linkCommands.configs.length ){
				//This is probably the last configuration.
				( linkCommands.configs || linkCommands.configs = [ ] ).push( config );	
			}

			if( linkCommands.procedures.length != linkCommands.configs.length ){
				//We have a big problem here.
				throw new Error( "procedure and configuration mismatched" );
			}

			//Aggregate the procedures and configs
			var operations = [];
			for( var index in linkCommands.procedures ){
				operations.push( {
					procedure: linkCommands.procedures[ index ],
					config: linkCommands.configs[ index ]
				} );
			}

			//This is I think necessary.
			//Procedures and configs should be volatile.
			delete linkCommands.procedures;
			delete linkCommands.configs;

			//This will be a complex composite function.
			var composite;
			var config;
			var procedure;

			var constructCompositiveFunction = function( config, procedure ){
				return ( function( result, callback ){
					//Save the result in the link commands
					linkCommands.lastResult = result;

					//Override the callback with this intelligent one :)
					config.callback = function( ){

						//Convert first the argument into an array.
						var parameters = Array.prototype.slice.call( arguments );

						/*
							The original callback will actually call the specified callback
								in the configuration corresponding to the procedure.
						*/
						var callOriginalCallback = function( ){
							//Take into account if we have original callbacks.
							//Override it but still execute it.
							if( config.callback ){
								config.callback.apply( linkCommands, parameters );
							}
						};

						//Search the argument if there is an Error parameter.
						//This is the safest way to do it.
						for( var _index in parameters ){
							if( parameters[ _index ] instanceOf Error ){
								callOriginalCallback( );
								callback( parameters[ _index ] );
								return;
							}
						}

						//If none then add a null to the first index
						parameter = parameter.slice( 0, null );
						
						callOriginalCallback( );
						callback.apply( linkCommands, parameter );
					}

					//Main execution here.
					procedure( config );
				} );
			}

			var nextFunction;
			var baseFunction = constructCompositiveFunction( operations[ 0 ].config,
				operations[ 0 ].procedure );
			for( var index = 1; index < operations.length; ){
				nextFunction = constructCompositiveFunction( operations[ index ].config,
					operations[ index ].procedure );
				composite = _.async.compose( nextFunction, composite || baseFunction );
			}

			//We are now ready to execute the complex composite function.
			composite
		};

		return chainFunction;
	}catch( error ){

	}
}

/*
	This running app is called a LINKER.

	A linker is always associated to a cloud service.

	The linker uses the cloud service to link its storage services.

	So basically, linkers will be used just for linking storage services.

	A linker can have many link servers.
	Each link server can only have one link backup database server.

	A link server can host many apps.

	A database in the link server can have many collections.

	One collection pertains to one usage of a user.

	Linkers can be linked to other linkers through a PATH LINK

	A path link is an identification that the two linkers are communicating.

	A linker can have many path links.

*/
function loadDb( host, port, callback ){

	var linkCommands = constructLinkCommands( this );

	try{
		var constructMongoDbCommand = function( path, host, port ){
			return ( function( config ){
				
				path = config.path || path;
				host = config.host || host;
				port = config.port || port;

				config = config || {
					logAppend: true,
					fork: true,
					directoryPerDb: true,
					journaling: true
				};

				config.logPath = config.logPath || ( path + "/log" );
				config.pidFilePath = config.pidFilePath || ( path + "/pid" );

				var command = "mongod ";
				
				if( config.fork ){
					command += "--fork ";
				}

				//General options
				command += ( "--dbpath " + path + " --port " + port + " --bind_ip " + host + " " ); 

				if( config.pidFilePath ){
					command += "--pidfilepath " + config.pidFilePath;
				}

				if( config.directoryPerDb ){
					command += "--directoryperdb ";
				}

				if( config.logPath ){
					command += "--logpath " + config.logPath;
				}

				if( config.logAppend ){
					command += "--logappend ";
				}

				if( config.journaling ){
					command += "--journal";
				}else{
					command += "--nojournal";
				}

				return command;
			} );
		};

		var constructDbInfoFromPIDFile = function( pidFile ){
			var dbServerInfo =  ( pidFile += "" ).replace( /\s+/g, "|" ).split( "|" );
			
			var _dbServerInfo = dbServerInfo;

			dbServerInfo = {
				pid: parseInt( dbServerInfo[ 0 ] ),
				id: dbServerInfo[ 1 ]
				name: dbServerInfo[ 2 ]
				port: dbServerInfo[ 3 ],
				host: dbServerInfo[ 4 ],
				hash: dbServerInfo[ 5 ]
			};

			//Fixed hash will be used for comparison.
			dbServerInfo.fixedHash = _.crypto.createHash( "md5" )
				.update( JSON.stringify( {
						pid: dbServerInfo.pid,
						id: dbServerInfo.id,
						name: dbServerInfo.name,
						port: dbServerInfo.port,
						host: dbServerInfo.host
					} ), "utf8" )
				.digest( "hex" ).toString( );

			_dbServerInfo = _dbServerInfo.splice( 0, 6 );

			dbServerInfo.databases = _dbServerInfo;

			return dbServerInfo;
		};

		var verifyPIDFromProcessList = function( pid, callback ){
			if( _.platformType == "win32" ){	
				_.process.exec( "tasklist /v /fi \"pid eq " + dbServerInfo.pid + "\"",
					function( error, stdout, stderr ){
						if( error ){
							return callback( error );
						}
						
						stdout = ( ( ( stdout += "" ).split( /[\n\r]+/g )[ 3 ] || "" )
							.split( /\s+/g )[ 1 ] || "" ).replace( /\s+/g, "" );
						
						pid += "";

						callback( pid == stdout && !!~pid.indexOf( stdout ) );		
					} );
			}else{
				_.process.exec( "ps aux | grep mongod | grep 'port'",
					function( error, stdout, stderr ){
						if( error ){
							return callback( error );
						}

						stdout = ( stdout += "" ).split( "\n" );
						for( var index in stdout ){
							if( !~stdout[ index ].indexOf( "ps aux" ) ){
								stdout = stdout[ index ].split( /\s+/ )[ 1 ];
								break;
							}
						}

						callback( parseInt( stdout ) == pid );
					} );
			}
		};

		var getAllRunningBackUpDbServers = function( path, callback ){
			/*
				Load database if database is not existing.
				This database is used as a back up database if the delegator
					didn't provide a collection.
				The collection name will depend on the app ID given.
				There will be one to one correspondence between a server
					and a database server.

				If a collection is specified. This will not use the back up database.

				This does not concern the integrity of the collection given.
					Therefore, any scenario arising from that given collection,
					this app will either just throw an error and do nothing.
			*/

			/*
				First we need to get the PID from a pid file
				Note that the path here must point to the root path where 
					all database servers are located.
			*/
			_.fs.readdir( path || "./linkdb",
			function( error, files ){
				
				if( error ){
					return callback( error );
				}

				_.async.map( files,
				function( file, cacheDirectory ){
					//If this doesn't work append the root path
					_.fs.stat( file,
						function( error, statistics ){
							if( error ){
								return cacheDirectory( );
							}

							if( statistics.isDirectory( ) ){
								return cacheDirectory( null, file );
							}

							return cacheDirectory( );
						} );
				},
				function( error, directories ){
					if( error ){
						return callback( error );
					}
					
					//Splice all empties.
					for( var index = 0; index < directories.length; index++ ){
						if( !directories[ index ] ){
							directories = directories.splice( index-- );
						}
					}

					//Now we have a list of database server directories.
					//Go inside and look for pid files
					_.async.map( directories,
					function( directory, cacheDbServerInfo ){
						_.fs.readFile( directory + "/pid",
						function( error, pidFile ){
							if( error ){
								return callback( error );
							}

							if( !~( pidFile + "" ).match( /\d\w/g ).length ){
								return cacheDbServerInfo( );
							}

							/*
								PID files may contain the following information in this order:

								1. pid
								2. id
								3. name
								4. port
								5. host
								6. hash
								7. databases
							*/

							var dbServerInfo =  constructDbInfoFromPIDFile( pidFile );
							
							verifyPIDFromProcessList( dbServerInfo.pid,
								function( result ){
									if( result instanceof Error ){
										return callback( result );
									}
									if( result ){
										return cacheDbServerInfo( null, dbServerInfo );
									}

									dbServerInfo.isNotalive = true;
									cacheDbServerInfo( null, dbServerInfo );
								} );			
						} );	
					},
					function( error, dbServers ){
						if( error ){
							return callback( error );
						}

						//Splice all nulls.
						//Record all dead servers and alive servers.
						for( var index = 0; index < dbServers.length; index++ ){
							if( !dbServers[ index ] ){
								dbServers = dbServers.splice( index-- );
							}else if( dbServers[ index ].isNotalive ){
								( _.deadDbServers = _.deadDbServers || [] )
									.push( dbServers[ index ] );
							}else{
								( _.aliveDbServers = _.deadDbServers || [] )
									.push( dbServers[ index ] );
							}
						}

						callback( _.aliveDbServers );
					} );
				} );
			} );
		};

		var onErrorCallback = function( error, callback ){
			if( !callback.called ){
				callback.called = true;
				callback( error );
			}
		};

		var createBackUpDbServer = function( host, port, callback ){
			return ( function( config ){
				config = config || {};

				//The path here is the root path.
				config.path = config.path || "./linkdb";

				var dbServerName = config.dbServerName || config.id;

				var dbServerFolder = config.path + "/" + dbServerName;

				var mongoDbCommand = constructMongoDbCommand( dbServerFolder, host, port )( config );

				var dbServerProcess;

				var startBackUpDbServer = function( ){
					dbServerProcess = _.process.exec( mongoDbCommand,
						function( error, stdout, stderr ){
							if( error ){
								return onErrorCallback( error, callback );
							}
						} );
				}

				_.fs.exists( dbServerFolder + "/pid",
					function( result ){
						if( result ){
							_.fs.unlink( dbServerFolder + "/pid",
								function( error ){
									if( error ){
										return onErrorCallback( error, callback );
									}
									startBackUpDbServer( );
								} );
							return;
						}
						startBackUpDbServer( );
					} );

				dbServerProcess.stdout.on( "data",
					function( data ){
						//Inspect the data if it is already alive and listening.
						if( !!~data.indexOf( "waiting for connections" ) 
							&& !dbServerProcess.hasSignaledOnWaiting )
						{
							dbServerProcess.hasSignaledOnWaiting = true;

							_.fs.readFile( dbServerFolder + "/pid",
								function( error, pid ){
									if( error ){
										return onErrorCallback( error, callback );
									}

									pid = parseInt( pid.replace( /\s+/g, "" ) );

									/*
										Record on the PID file the id, name, host and 
											port information.
										This procedure is static. Attempt to edit makes the 
											database server inaccessible.
									*/
									var hash = _.crypto.createHash( "md5" )
										.update( JSON.stringify( {
												pid: pid,
												id: config.id,
												name: dbServerName,
												port: port,
												host: host
											} ), "utf8" )
										.digest( "hex" ).toString( );

									_.fs.appendFile( dbServerFolder + "/pid",
										( config.id + "\n" +
											dbServerName + "\n" +
											host + "\n" +
											port + "\n" +
											hash ),
										function( error ){
											if( error ){
												return onErrorCallback( error, callback );
											}

											callback( {
												hash: hash,
												process: dbServerProcess,
												folder: dbServerFolder
											} );
										} );		
								} );
						}
					} );
				
				dbServerProcess.stderr.on( "data",
					function( data ){
						//For every error shut down the database.
						dbServerProcess.kill( "SIGTERM" );
						if( !callback.called ){
							callback.called = true;
							callback( new Error( ( data += "" ) ) );
						}
					} );

				dbServerProcess.on( "exit",
					function( code ){
						if( config.onDbServerCloseHandler ){
							config.onDbServerCloseHandler( ( host + ":" + port ), code );
						}
					} );
			} );
		};

		return ( function( config ){

			config = config || {};

			host = config.host || host;
			port = config.port || port;
			callback = config.callback || callback;

			if( !host || !port || !callback ){
				var error = new Error( "invalid parameter values" );
				if( callback ){
					return callback( error );
				}
				throw error;
			}

			_.dbList = _.dbList || {};

			var dbServerInfo = config.dbServerInfo || _.dbList[ host + ":" + port ];

			if( !_.dbList[ dbServerInfo.host + ":" + dbServerInfo.port ] ){
				_.dbList[ dbServerInfo.host + ":" + dbServerInfo.port ] = dbServerInfo;
			}

			dbServerInfo.id = config.id || dbServerInfo.id || _.crypto.createHash( "md5" )
				.update( _.uuid.v4( ), "utf8" )
				.digest( "hex" ).toString( );

			dbServerInfo.name = config.name || dbServerInfo.name || dbServerInfo.id;

			//Check if the db is in the list
			if( dbServerInfo ){
				//Check if database server is running.	
				verifyPIDFromProcessList( dbServerInfo.pid,
					function( result ){
						if( result instanceof Error ){
							return callback( result );
						}

						if( result ){
							//The database is alive.
							dbServerInfo.lastCheckAliveDate = Date.now( );
							linkCommands.selectedDb = dbServerInfo;
							callback( dbServerInfo );
						}else{
							//The database is not alive so creating it.
							createBackUpDbServer( host, port,
								function( result ){
									if( result instanceof Error ){
										return callback( result );
									}

									dbServerInfo.hash = result.hash;
									dbServerInfo.proces = result.process;
									dbServerInfo.folder = result.folder;

									dbServerInfo.lastCheckAliveDate = Date.now( );
									linkCommands.selectedDb = dbServerInfo;
									callback( dbServerInfo );
								} )( config );
						}
					} );
			}else{
				//Proceed checking if a back up db server exists.
				//This will randomnly select a database server.

			}

			return linkCommands;
		} ), ( config. );
	}catch( error ){

	}
}
exports.loadDb = loadDb;

/*
	Construct a dynamic bind link to the app for easy access of link commands.
*/
function link( appID ){

	//Create a link commands

	var linkCommands = constructLinkCommands( this );

	try{
		return ( function( config ){
			config = config || { };

			//If the delegator suggest other modules.
			_ = _.merge( config._, config.forceOverrideModules );
			

			if( config.referenceID ){

			}

			if( config.defaultAppInfo ){
				_.defaultAppKey = config.defaultAppInfo.key;
				_.defaultAppSecret = config.defaultAppInfo.secret;
			}
			
			if( appID ){
				//This is a search function for app using the app ID.
				linkCommands.selectedApp = StorageLink.getApp( appID );
			}
			
			return linkCommands;
		} );
	}catch( error ){

	}finally{

	}
};

function StorageLink( host, port, collection, app ){
	_.addressList = _.addressList || {};
	if( !_.addressList[ host + ":" + port ] ){
		
		//Are we using the old or new storage collection?
		collection.storage.id = collection.storage.id 
			|| _.crypto.createHash( "md5" )
				.update( _.uuid.v4( ), "utf8" )
				.digest( "hex" ).toString( );

		var self = this;

		//This will give us a fresh storage information object.
		var newStorageInfo = function( ){
			return ( self.storageInfo = {
				storage: collection.storage,
				name: collection.name,
				id: collection.storage.id
			} );
		};

		//This will give us a fresh server information object.
		var newServerInfo = function( ){
			_.addressList[ host + ":" + port ] = {
				host: host,
				port: port,
				url: "http://" + host + ( ( port != 80 )? "" : ":" + port )
			};

			_.addressList[ host + ":" + port ].storage = {};
			_.addressList[ host + ":" + port ]
				.storage[ collection.name + ":" + collection.storage.id ] = newStorageInfo( );

			return _.addressList[ host + ":" + port ];
		}

		//Are we using the old server or create a new one.
		this.serverInfo = _.addressList[ host + ":" + port ] || newServerInfo( );

		/*
			Use an existing storage or add a new one?
			A server can have multiple storages (collection in the database).
			Different servers may refer to the same storage.
			This provides flexibility of access if a server is down, 
				the collection can still be accessed from other servers.
		*/
		if( !this.serverInfo.storage[ collection.name + ":" + collection.storage.id ] ){
			this.serverInfo
				.storage[ collection.name + ":" + collection.storage.id ] = newStorageInfo( );
		}

		//Are we referencing to the old app ID or construct a new one?
		this.appInfo = StorageLink.getApp( app.id ) 
			|| ( function( ){
				( self.serverInfo.apps = self.serverInfo.apps || [] )
					.push( self.appInfo = {
						key: app.key,
						secret: app.secret,
						id: _.crypto.createHash( "md5" )
							.update( _.uuid.v4( ), "utf8" )
							.digest( "hex" ).toString( )
					} );

				/*
					This is kind of complex thing.
					We have 2 global storage, the addressList and the serverList

					Address List focuses on the servers information
					Server List focuses on the apps each server has.
					A server can have many apps.
					An app can be hosted on many servers.
					
					This sounds a big why but I think this provides flexibility.
				*/

				_.serverList = _.serverList || {};
				_.serverList[ self.appInfo.id ] = self.serverInfo;

				return self.appInfo;
			} )( );
		
		
		( this.serverInfo.liveServer = _.http.createServer( )
			.on( "request",
				function( request, response ){
					switch( _.url.parse( request.url ).pathname.match( /[^\/].+/ )[ 0 ] ){
						case "authorize":
							authorize( response, 
								function( linkID, requestToken ){

								} );
							break;
						case "authorize/dropbox/callback":
							console.log( JSON.stringify( _.url.parse( request.url, true ).query ) );
							/*authorize( response, function( ){

							} )*/
					}
				} )
			.on( "listening",
				function( ){
					console.log( "Server listening." );
					
					console.log( "Initializing dropbox configurations." );

					self.appInfo.liveApp = _.dbox.app( {
						app_key: self.appInfo.key, 
						app_secret: self.appInfo.secret 
					} );

					self.isDropboxInitialized = true;
					self.isAlive = true;
					self.aliveTime = Date.now( );
				} ) )
			.listen( _.defaultPort, _.defaultIP );
	}else{

	}
}
exports.link = link;

StorageLink.getApp = function( appID ){
	if( !~Object.keys( _.serverList || {} ).length ) return;
	
	for( var index in _.serverList[ appID ].app ){
		if( _.serverList[ appID ].app[ index ].id == appID ){
			return _.serverList[ appID ].apps[ index ];
		}
	}
}

function authorize( response, callback ){
	var self = this;
	try{
		return ( function( config ){

			config = config || { };

			if( self.selectedApp ){

			}else if( config.serverInfo ){
				//Check if we have a collection.
				if( !config.collection ){

				}

				/*
					Provide defaults app information.
					Override the last default app information for custom last resort.
				*/
				if( !config.app ){
					config.app = config.app || { };
					config.app.key = _.defaultAppKey || "8hzbh86z4icabe0";
					config.app.secret = _.defaultAppSecret || "39zfeghuuktogq0";
				}

				//Create or check if a server already exists.
				var storage = new StorageLink( config.serverInfo.host,
					config.serverInfo.port,
					config.collection,
					config.app );
			}

			if( _.isDropboxInitialized ){
				_.app.requesttoken( function( status, requestToken ){
					if( status == 200 ){
						//console.log( JSON.stringify( requestToken ) );
						//Generate a id link appended to the URL
						var linkID;
						try{
							linkID = _.crypto.createHash( "md5" )
								.update( _.uuid.v4( ), "utf8" )
								.digest( "hex" ).toString( );
						}catch( error ){

						}
						response.writeHead( 302, {
							Location: requestToken.authorize_url 
								+ "&oauth_callback=" + _.callbackURL + "?linkID=" + linkID
						} );
						response.end( );
						
						callback( linkID, requesToken );
					}
				} );
			}
		} );
	}catch( error ){

	}
}
exports.authorize = authorize;

/*
	This add function has also capability to
		1. update
		2. chunkify
*/
function add( filePath ){

}
exports.add = add;


function remove( filePath ){

}
exports.remove = remove;

function check( ){

}
exports.check = check;