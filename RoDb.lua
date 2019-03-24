local DataStoreService = game:GetService("DataStoreService")
local Players = game:GetService('Players')
local Type = require(game.ServerStorage.Library.Utility.Type)
local RoDb = {}
local datastoreCache = {}
local playerTableCache = {}
local type = type
local pcall = pcall
local pairs = pairs
local warn=warn

local function getPlayerId(player)
	return 'p_'..player.UserId
end

local function getPlayerTableCache(tableName,player)
	local success,dbTable = pcall(function()
		return playerTableCache[tableName][getPlayerId(player)]
	end)
	if success then return dbTable
	end
	
	return false
end

local function getDatastore(datastoreName)
	if datastoreCache[datastoreName] then
		return datastoreCache[datastoreName]
	end
	return false
end


function RoDb:LoadDatabase(datastoreName)
	local dbTable = DataStoreService:GetDataStore(datastoreName)
	datastoreCache[datastoreName] = dbTable
end

function RoDb:LoadDatabases(datastoreNames)
	for _, dbName in pairs(datastoreNames) do
		local dbTable = DataStoreService:GetDataStore(dbName)
		datastoreCache[dbName] = dbTable
	end
end

function RoDb:LoadPlayerTable(datastore,tableName,player)
	
	local success,result = pcall(function()
		local database = datastoreCache[datastore]
		local data = database:GetAsync(getPlayerId(player))
		if data then
			if not playerTableCache[tableName] then
				playerTableCache[tableName] = {}
			end
			playerTableCache[tableName][getPlayerId(player)]=data
			return true 
		else
			warn('Table '..tableName..' failed to load for '..player.Name)
			return false
		end
		return false
	end)
	
	if success then return result end
	return false
	
end




function RoDb:AddTableEntry(tableName,player,key,value)
	local dbTable = getPlayerTableCache(tableName,player)
	
	if dbTable then
		if dbTable[key] then
			warn("Entry already exists.")
		else
			dbTable[key]=value
		end
		
	end	
end

function RoDb:UpdateTableEntry(tableName,player,key,updatedValue)
	local dbTable = getPlayerTableCache(tableName,player)
	
	if dbTable then
		if dbTable[key] then
			local currentValue = dbTable[key]
			if Type.Compare(currentValue,updatedValue) then
				warn('Type mismatch when updating entry')
			else
				dbTable[key] = updatedValue
			end
			
			
		else
			warn("Entry does not exists.")
		end
		
	end	
end

function RoDb:GetTableEntry(tableName,player,key)
	local dbTable = getPlayerTableCache(tableName,player)
	
	if dbTable then
		return dbTable[key]
	end
	return false
end

function RoDb:SaveTable(datastore,tableName,player)
	local dbTable = getPlayerTableCache(tableName,player)
	if dbTable then
		local dstore = getDatastore(datastore)
		if dstore then
			local success, err = pcall(function()
				dstore:SetAsync(getPlayerId(player),dbTable)
			end)
			if not success then
				warn('Cannot save player data!')
			end
		else
			warn('Datastore does not exist!')
		end
	else
		warn('Table doesnt exist for player!')
	end
end

function RoDb:SaveAllTables(datastore,tableNames,player)
	if Type.Table(tableNames) then
		for _, t in pairs(tableNames) do
			RoDb.SaveTable(datastore,t,player)
		end
	end
end



return RoDb