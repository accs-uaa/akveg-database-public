-- -*- coding: utf-8 -*-
-- ---------------------------------------------------------------------------
-- Insert project metadata
-- Author: Timm Nawrocki, Alaska Center for Conservation Science
-- Last Updated: 2021-02-02
-- Usage: Script should be executed in a PostgreSQL 12 database.
-- Description: "Insert project metadata" pushes the metadata for all projects into the project table of the database.
-- ---------------------------------------------------------------------------

-- Initialize transaction
START TRANSACTION;

-- Insert project data into project table
INSERT INTO project (project_id, originator_id, funder_id, manager_id, project_name, project_abbr, completion_id, year_start, year_end, project_description) VALUES
(1, 2, 5, 81, 'Assessment, Inventory, and Monitoring Pilot for National Petroleum Reserve - Alaska', 'AIM NPR-A', 1, 2012, 2017, 'Establishment and measure of long-term monitoring plots for the BLM Assessment, Inventory, and Monitoring Program in National Petroleum Reserve - Alaska.'),
(2, 2, 3, 80, 'Colville River Small Mammal Surveys', 'ACCS Colville', 1, 2015, 2015, 'Vegetation plots assessed during small mammal surveys conducted along the Colville River in 2015 by ACCS.'),
(3, 13, 8, 6, 'Balsam Poplar Communities on the Arctic Slope of Alaska', 'Breen Poplar', 1, 2003, 2006, 'The vegetation associated with balsam poplar stands in the Arctic Foothills of Alaska and the interior boreal forests of Alaska and Yukon was described by Breen (2014) as part of her doctoral dissertation research.'),
(4, 2, 7, 40, 'Landsat Derived Map and Landcover Descriptions for Gates of the Arctic National Park and Preserve', 'NPS Gates LC', 1, 1998, 1999, 'Ground and aerial plots collected for the creation of a land cover map for Gates of the Arctic National Park and Preserve.'),
(5, 9, 9, 33, 'North Slope Land Cover', 'North Slope LC', 1, 2008, 2011, 'Ground plots for the creation of a land cover map and plant associations for the North Slope.'),
(6, 2, 7, 40, 'Plant Associations and Post-fire Succession in Yukon-Charley Rivers National Preserve', 'NPS Yukon-Charley PA', 1, 2003, 2003, 'Ground plots collected to describe plant associations of Yukon-Charley Rivers National Preserve.'),
(7, 2, 5, 81, 'Fortymile River Region Assessment, Inventory, and Monitoring', 'AIM Fortymile', 1, 2016, 2017, 'Establishment and measure of long-term monitoring plots for the BLM Assessment, Inventory, and Monitoring Program in Fortymile River Region of Eastern Interior Field Office (EIFO).'),
(8, 7, 7, 58, 'Lichen Inventory of the National Park Service Arctic Network', 'NPS ARCN Lichen', 1, 1996, 2007, 'Lichen and bryophyte ground plots for describing lichen community structure and its relation to environment in NPS Arctic Network.'),
(9, 10, 10, 76, 'Plant Associations of the Selawik National Wildlife Refuge', 'USFWS SELA PA', 1, 2005, 2005, 'Vegetation plots collected to classify plant associations of the Selawik National Wildlife Refuge by Stephen Talbot.'),
(10, 10, 10, 76, 'Selawik National Wildlife Refuge Land Cover', 'USFWS Selawik LC', 1, 1996, 1998, 'Ground plot data collected by USFWS in Selawik National Wildlife Refuge for development of a land cover map.'),
(11, 10, 10, 61, 'Vegetation Monitoring in Interior Refuges', 'USFWS Interior', 1, 2013, 2014, 'Vegetation plot data collected in Interior Alaska Refuges as part of the Alaska Regional Refuge Inventory and Monitoring Strategic Plan.'),
(12, 2, 7, 40, 'Land Cover and Plant Associations of Denali National Park and Preserve', 'NPS Denali LC', 1, 1998, 1999, 'Ground plots collected for the creation of a land cover map and plant associations for Denali National Park and Preserve.'),
(13, 2, 7, 40, 'Alagnak Wild River Land Cover and Plant Associations', 'NPS Alagnak LC', 1, 2010, 2014, 'Ground plots collected for the creation of a land cover map and plant associations for Alagnak National Wild River.'),
(14, 2, 7, 40, 'Landcover Classes, Ecoregions, and Plant Associations of Katmai National Park and Preserve', 'NPS Katmai LC', 1, 2000, 2003, 'Ground plots collected for the creation of a land cover map and plant associations for Katmai National Park and Preserve.'),
(15, 2, 7, 40, 'Plant Associations, Vegetation Succession, and Earth Cover Classes of Aniakchak National Monument and Preserve', 'NPS Aniakchak LC', 1, 2009, 2012, 'Ground plots collected for the creation of a land cover map and plant associations for Aniakchak National Monument and Preserve.'),
(16, 2, 5, 29, 'GMT-2 Assessment, Inventory, and Monitoring', 'AIM GMT-2', 2, 2019, 2021, 'Establishment of vegetation monitoring plots within the Greater Mooses Tooth 2 Oil and Gas Lease Area for BLM Assessment, Inventory, and Monitoring.'),
(17, 2, 3, 80, 'Bristol Bay Vegetation Cover', 'ACCS Bristol Bay VC', 1, 2019, 2019, 'Vegetation plots with focus towards likely moose habitat for development of species- or aggregate-level foliar cover maps.'),
(18, 2, 2, 80, 'Vegetation Mapping of North American Beringia', 'ACCS Beringia VC', 2, 2019, NULL, 'Vegetation plots collected for development of species- or aggregate-level foliar cover maps and other quantitative vegetation maps of North American Beringia.'),
(19, 2, 7, 40, 'Landcover Classes, Ecological Systems, and Plant Associations of Kenai Fjords National Park', 'NPS Kenai Fjords LC', 1, 2004, 2008, 'Ground and aerial plots collected for the creation of a land cover map and plant associations for Kenai Fjords National Park.'),
(20, 2, 7, 40, 'Landcover Classes and Plant Associations for Glacier Bay National Park and Preserve', 'NPS Glacier Bay LC', 1, 2001, 2008, 'Ground plots collected for the creation of a land cover map and plant associations for Kenai Fjords National Park.'),
(21, 2, 7, 81, 'Klondike Gold Rush National Park Land Cover', 'NPS Klondike LC', 1, 2011, 2012, 'Ground plots collected for the creation of a land cover map and plant associations for Klondike Gold Rush National Park.'),
(22, 2, 7, 46, 'Landcover Classes of Sitka National Historic Park', 'NPS Sitka LC', 1, 2011, 2013, 'Ground plots collected for the creation of a land cover map for Sitka National Historic Park.'),
(23, 7, 7, 83, 'Katmai Bear Habitat', 'Katmai Bear', 1, 1993, 1998, 'Data for 495 plots were collected by Dr. Tom Smith (USGS Biological Resource Division) and crew for the Katmai National Park Land Cover Classification Project and Brown Bear Habitat Analysis.'),
(24, 5, 15, 17, 'Dalton Highway Corridor Earth Cover', 'Dalton EC', 1, 2002, 2002, 'Aerial plots collected for the creation of an earth cover map for the Dalton Highway Corridor region.'),
(25, 5, 15, 17, 'Galena Military Operations Area and Nowitna National Wildlife Refuge Earth Cover', 'Galena EC', 1, 2000, 2000, 'Aerial plots collected for the creation of an earth cover map for the Galena Military Operations Area and Nowitna National Wildlife Refuge. Project was a multipartner effort among Bureau of Land Management, Ducks Unlimited Inc., and U.S. Fish and Wildlife Service.'),
(26, 5, 15, 17, 'Goodnews Bay Earth Cover', 'Goodnews EC', 1, 2001, 2001, 'Aerial plots collected for the creation of an earth cover map for the Goodnews Bay region. Project was a multipartner effort among Bureau of Land Management, Ducks Unlimited Inc., and U.S. Fish and Wildlife Service.'),
(27, 5, 15, 17, 'Gulkana Earth Cover', 'Gulkana EC', 1, 1997, 1997, 'Aerial plots collected for the creation of an earth cover map for the Gulkana region. Project was a multipartner effort among Bureau of Land Management, Ducks Unlimited Inc., and U.S. Department of Defense.'),
(28, 5, 15, 17, 'Haines Earth Cover', 'Haines EC', 1, 2000, 2000, 'Aerial plots collected for the creation of an earth cover map for the Haines region.'),
(29, 10, 15, 17, 'Innoko National Wildlife Refuge Earth Cover', 'Innoko EC', 1, 1998, 1998, 'Aerial plots collected for the creation of an earth cover map for the Innoko National Wildlife Refuge and surrounding region. Project was a multipartner effort among Bureau of Land Management, Ducks Unlimited Inc., and U.S. Fish and Wildlife Service.'),
(30, 10, 15, 17, 'Kanuti National Wildlife Refuge Earth Cover', 'Kanuti EC', 1, 1998, 1998, 'Aerial plots collected for the creation of an earth cover map for the Kanuti National Wildlife Refuge and surrounding region. Project was a multipartner effort among Bureau of Land Management, Ducks Unlimited Inc., and U.S. Fish and Wildlife Service.'),
(31, 5, 15, 17, 'Kenai Peninsula Earth Cover', 'Kenai EC', 1, 1998, 1998, 'Aerial plots collected for the creation of an earth cover map for the Kenai Peninsula. Project was a multipartner effort among Ducks Unlimited Inc., U.S. Fish and Wildlife Service, National Park Service, Alaska Department of Fish and Game, Alaska Department of Natural Resources, U.S. Forest Service, Kenai Borough, Spatial Solutions Inc., and Bureau of Land Management.'),
(32, 5, 15, 17, 'Kvichak Earth Cover', 'Kvichak EC', 1, 2001, 2001, 'Aerial plots collected for the creation of an earth cover map for the Kvichak River region.'),
(33, 10, 15, 17, 'Melozitna River and Koyukuk National Wildlife Refuge Earth Cover', 'Melozitna EC', 1, 2001, 2001, 'Aerial plots collected for the creation of an earth cover map for the Melozitna River region and Koyukuk National Wildlife Refuge. Project was a multipartner effort among Bureau of Land Management, Ducks Unlimited Inc., and U.S. Fish and Wildlife Service.'),
(34, 5, 15, 17, 'Naknek Military Operations Area Earth Cover', 'Naknek EC', 1, 2000, 2000, 'Aerial plots collected for the creation of an earth cover map for the Naknek Military Operations Area. Project was a multipartner effort among Bureau of Land Management, Ducks Unlimited Inc., and U.S. Department of Defense.'),
(35, 5, 15, 17, 'Northern Innoko Earth Cover', 'Northern Innoko EC', 1, 1999, 1999, 'Aerial plots collected for the creation of an earth cover map for the northern Innoko National Wildlife Refuge and surrounding region. Project was a multipartner effort among Bureau of Land Management, Ducks Unlimited Inc., and U.S. Fish and Wildlife Service.'),
(36, 5, 15, 17, 'Northern Yukon Military Operations Area Earth Cover', 'Northern Yukon EC', 1, 1999, 2000, 'Aerial plots collected for the creation of an earth cover map for the Northern Yukon Military Operations Area. Project was a multipartner effort among Bureau of Land Management, Ducks Unlimited Inc., and U.S. Department of Defense.'),
(37, 5, 15, 17, 'Seward Peninsula Earth Cover', 'Seward Peninsula EC', 1, 2003, 2003, 'Aerial plots collected for the creation of an earth cover map for the Seward Peninsula.'),
(38, 5, 15, 17, 'Southern Yukon Military Operations Area Earth Cover', 'Southern Yukon EC', 1, 1999, 2000, 'Aerial plots collected for the creation of an earth cover map for the Southern Yukon Military Operations Area. Project was a multipartner effort among Bureau of Land Management, Ducks Unlimited Inc., and U.S. Department of Defense.'),
(39, 5, 15, 17, 'Stoney River Military Operations Area Earth Cover', 'Stoney River EC', 1, 1999, 1999, 'Aerial plots collected for the creation of an earth cover map for the Stoney River Military Operations Area. Project was a multipartner effort among Bureau of Land Management, Ducks Unlimited Inc., and U.S. Department of Defense.'),
(40, 5, 15, 17, 'Susitna Military Operations Area Earth Cover', 'Susitna EC', 1, 1999, 1999, 'Aerial plots collected for the creation of an earth cover map for the Susitna Military Operations Area. Project was a multipartner effort among Bureau of Land Management, Ducks Unlimited Inc., and U.S. Department of Defense.'),
(41, 5, 15, 17, 'Tanana Flats Earth Cover', 'Tanana Flats EC', 1, 1994, 1995, 'Aerial plots collected for the creation of an earth cover map for the Tanana Flats. Project was a multipartner effort among Bureau of Land Management, Ducks Unlimited Inc., U.S. Fish and Wildlife Service, and U.S. Department of Defense.'),
(42, 10, 15, 17, 'Tetlin National Wildlife Refuge Earth Cover', 'Tetlin EC', 1, 2005, 2005, 'Aerial plots collected for the creation of an earth cover map for the Tetlin National Wildlife Refuge.'),
(43, 5, 15, 17, 'Tiekel River Earth Cover', 'Tiekel EC', 1, 1998, 1998, 'Aerial plots collected for the creation of an earth cover map for the Tiekel River region.'),
(44, 10, 15, 17, 'Yukon Delta National Wildlife Refuge', 'Yukon Delta EC', 1, 2006, 2006, 'Aerial plots collected for the creation of an earth cover map for the Yukon Delta National Wildlife Refuge.'),
(45, 11, 15, 17, 'Stikine River Earth Cover', 'Stikine EC', 1, 2007, 2007, 'Aerial plots collected for the creation of an earth cover map for the Stikine River region.'),
(46, 7, 7, 33, 'Bering Land Bridge National Monument Land Cover', 'Bering LC', 1, 2002, 2003, 'Aerial plots collected for the creation of a land cover map for the Bering Land Bridge National Monument.'),
(47, 7, 7, 33, 'Wrangell-St. Elias National Park and Preserve Land Cover', 'Wrangell LC', 1, 2004, 2006, 'Aerial plots collected for the creation of a land cover map for the Wrangell-St. Elias National Park and Preserve.');

-- Commit transaction
COMMIT TRANSACTION;