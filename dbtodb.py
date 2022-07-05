import pymssql
import os
import keyboard
import time
import traceback
import copy
import configparser 
##
from datetime import datetime, date
##################################################################
from dateutil.relativedelta import relativedelta, FR, TU
from dateutil.easter import easter
from dateutil.parser import parse
from dateutil import rrule
###################################################################
##
from progress.bar import ShadyBar
#import sqlite3
####
from rich.console import Console
from rich.table import Table
from rich import print
from rich.traceback import install
from rich.panel import Panel
from rich import box
from rich.prompt import Prompt, IntPrompt
from rich.console import Group
#####################


##############
#rich setting#

install() #for tracebacks


console = Console()
rich_ok = "bright_green on black"
rich_error = "bright_red on black"
rich_yellow = "bright_yellow on black"
rich_sky_blue = "sky_blue3 on black"

#rich settings end#
###################

########
#config#
config = configparser.ConfigParser()
config.read("config.cnf")
#end config#
############

###########
#functions#
def cls():
    os.system('cls' if os.name=='nt' else 'clear')

def press_any_key():
    os.system('pause' if os.name=='nt' else "read -n 1 -s -r -p 'press any key'")
#functions end#
###############

database_num = 1
##
bool_to_main_while = True
while bool_to_main_while == True:
    ##
    cls()
    test_mode = config.getboolean("TEST_MODE", "test mode")

    #######
    #mssql#

    ##############
    #Roger Zabrze#

    host_to_viso = config.get("ROGER", "server") #'192.168.71.253\ROGER_ZABRZE'
    user_to_viso = config.get("ROGER", "user") #'roger-ro'
    password_to_viso = config.get("ROGER", "password") #'QE4tWEG#$5'
    database_to_viso = config.get("ROGER", "database") #'Viso'
    ####
    try:
        connection_to_viso = pymssql.connect(
                host=host_to_viso,
                user=user_to_viso,
                password=password_to_viso,
                database=database_to_viso)

        cursor_to_viso = connection_to_viso.cursor()
        console.print(Panel("db connect to viso ok"), style=rich_ok)

    except: 
        console.print(Panel("db connect to viso error"), style=rich_error)
    
    #Roger Zabrze end#
    ##################

    #######
    #enova#

    if test_mode == False:
        table_name_for_insert = 'WejsciaWyjsciaI'
        table_name_duplicate_for_insert = 'WejsciaWyjsciaO'
        #
        if database_num == 1:
            database_name_for_enova = config.get("ENOVA", "database_1") #'DromaSC78' #main base 
        elif database_num == 2:
            database_name_for_enova = config.get("ENOVA", "database_2") #'Droma'
        elif database_num == 3:
            database_name_for_enova = config.get("ENOVA", "database_3") #'DromaZW'
        elif database_num == 4:
            database_name_for_enova = config.get("ENOVA", "database_4") #'Faher'
    ##
    elif test_mode == True:
        table_name_for_insert = config.get("TEST_MODE", "table_for_test") #'test_copy_2'
        database_name_for_enova = config.get("TEST_MODE", "database_for_test") #'for_test_import'
    ####
    host_to_enova = config.get("ENOVA", "server") #'192.168.40.24\ENOVA'
    user_to_enova = config.get("ENOVA", "user") #'DJ'
    password_to_enova = config.get("ENOVA", "password") #'KREma.234.luka'
    database_to_enova = database_name_for_enova#'for_test_import'
    ##
    try:
        connection_to_enova = pymssql.connect(
                    host=host_to_enova,
                    user=user_to_enova,
                    password=password_to_enova,
                    database=database_to_enova)
        ##
        cursor_to_enova = connection_to_enova.cursor()
        console.print(Panel("db connect to enova ok"), style=rich_ok)
    except:
        console.print(Panel("error connect to enova"), style=rich_error)

    #enova end#
    ###########

    ################################
    #import data from viso to enova#

    finish_rows = [] 
    finish_rows_duplicate = []
    def import_data_from_viso_to_enova():
        global finish_rows
        global finish_rows_duplicate
        #
        try:
            cursor_to_viso.execute("SELECT MAX(id) FROM AccessUserPersons")
            rows = cursor_to_viso.fetchall()
        except:
            console.print(Panel("error to get maxID", expand=True), style=rich_error)
            console.print(Panel(traceback.format_exc()), style=rich_error)
            return 0
        #
        max_id = rows[0][0] + 1
        ####
        date1 = Prompt.ask("date 1 (YYYY-MM-DD)")#"2022-03-01"
        date2 = Prompt.ask("date 2 (YYYY-MM-DD)")#"2022-03-31"
        date1_datetime = datetime.strptime(date1, '%Y-%m-%d')
        date2_datetime = datetime.strptime(date2, '%Y-%m-%d')
        ##
        while date1_datetime.month != date2_datetime.month:
            cls()
            console.print("error")
            date1 = Prompt.ask("date 1 (YYYY-MM-DD)")#"2022-03-01"
            date2 = Prompt.ask("date 2 (YYYY-MM-DD)")#"2022-03-31"
            date1_datetime = datetime.strptime(date1, '%Y-%m-%d')
            date2_datetime = datetime.strptime(date2, '%Y-%m-%d')
        ##
        date_buff = datetime.strptime(date2, "%Y-%m-%d")
        date_buff = date_buff + relativedelta(days=+1)
        date3 = date_buff.strftime("%Y-%m-%d")
        ##
        
        #################
        #while in import#
        
        while_for_import = 4#1478
        errors = False
        #progress bar
        bar = ShadyBar('', max=max_id-while_for_import)
        while while_for_import < max_id:
            #console.print(while_for_import)

            ########
            #step 1#

            ############################
            #get UserExternalIdentifier#
            ############################

            try:
                cursor_to_viso.execute("SELECT AccessUserPersons.UserExternalIdentifier, "
                            "AccessUserPersons.ID "
                            "FROM AccessUserPersons "
                            "WHERE "
                            "AccessUserPersons.id = "+str(while_for_import)) 
                rows = cursor_to_viso.fetchall()
                user_external_id = copy.copy(rows[0][0])
            except:
                errors = True
                console.print(Panel("error get external id"), style=rich_error)
                console.print(Panel(traceback.format_exc()), style=rich_error)
                y_n = Prompt.ask("continue?", choices=['y', 'n'])
                if y_n == 'n':
                    return False 
            #end step 1#
            ############

            ##########
            #step 1.1#

            ##################
            #get RCP <---> ID#
            ##################

            if errors == False:
                if test_mode == False:
                    try:
                        cursor_to_enova.execute("""SELECT 
                            KartyRCP.Pracownik  
                            FROM KartyRCP 
                            WHERE 
                            KartyRCP.Numer = '"""+user_external_id+"'")
                        rows = cursor_to_enova.fetchall()
                        ##
                        if rows:
                            pracownik_in_enova = copy.copy(rows[0][0])
                        else:
                            errors = True
                    ##
                    except:
                        errors = True
                        console.print(Panel("error get RCP -> ID"), style=rich_error)
                        console.print(Panel(traceback.format_exc()), style=rich_error)
                        y_n = Prompt.ask("continue?", choices=['y', 'n'])
                        if y_n == 'n':
                            return False 
                ##
                elif test_mode == True:
                    pracownik_in_enova = user_external_id
            #end step 1.1#
            ##############

            ########
            #step 2#

            #################
            #get log entries#
            #################
            if errors == False: 
                try:
                    cursor_to_viso.execute("SELECT EventLogEntries.LoggedOn, "
                            "EventLogEntries.ControllerID "
                            "FROM "
                            "EventLogEntries "
                            "WHERE "
                            "EventLogEntries.PersonID = '"+str(while_for_import)+"' AND "
                            "EventLogEntries.LoggedOn >= '"+date1+"' AND EventLogEntries.LoggedOn <= '"+date3+"'") 
                    rows = cursor_to_viso.fetchall()
                except:
                    errors = True
                    console.print(Panel("error get logs entries"), style=rich_error)
                    console.print(Panel(traceback.format_exc()), style=rich_error)
                    y_n = Prompt.ask("continue?", choices=['y', 'n'])
                    if y_n == 'n':
                        return False 
            #end step 2#
            ############

            #if not empty#
            if rows:
                ########
                #step 3#

                ###########################
                #leave only log in log out#
                ###########################

                if errors == False:
                    i_in_while = 1
                    while i_in_while < len(rows)-1:
                        if rows[i_in_while][0].day == rows[i_in_while+1][0].day:
                            del rows[i_in_while]
                        else:
                            i_in_while += 2
                #end step 3#
                ############

                ########
                #step 4#
                
                ###################################
                #create rows for table in database#
                ###################################


                if errors == False:
                    i_in_while = 0
                    bool_for_typ = False
                    while i_in_while < len(rows):
                        ##
                        timestamp_buff = datetime.timestamp(rows[i_in_while][0])
                        time_sum_in_min = rows[i_in_while][0].hour*60+rows[i_in_while][0].minute
                        ##
                        typ = 0
                        operacia = 0
                        #
                        if bool_for_typ == False:
                            typ = 1
                            operacia = 0
                            bool_for_typ = True
                        else:
                            typ = 2
                            operacia = 16
                            bool_for_typ = False
                        ##
                        finish_row = [pracownik_in_enova, rows[i_in_while][0], time_sum_in_min, typ, operacia, 0, 0, 10, 0]
                        finish_row_duplicate = [pracownik_in_enova, rows[i_in_while][0], time_sum_in_min, typ, operacia, 10, 0]
                        ##
                        finish_rows.append(tuple(finish_row))
                        finish_rows_duplicate.append(tuple(finish_row_duplicate))
                        ##
                        i_in_while += 1
                        
                #end step 4#
                ############
                
                ########
                #step 5#

                ########################
                #write data in database#
                ########################

                if errors == False:
                    try:
                        buff_tuple = tuple(finish_rows)
                       #enova_columns: str = "Pracownik, Data, Godzina, Typ, Operacja, Zaimportowany, Stan, CzytnikRCP, Stamp, Zmodyfikowany"
                        cursor_to_enova.executemany("""INSERT INTO """+table_name_for_insert+""" 
                            (Pracownik, Data, Godzina, Typ, Operacja, Zaimportowany, Stan, CzytnikRCP, Zmodyfikowany)  
                            VALUES (%s, %d, %d, %d, %d, %d, %d, %d, %d)""", buff_tuple)
                        connection_to_enova.commit()
                        #duplicate#
                        if test_mode == False:
                            buff_tuple = tuple(finish_rows_duplicate)
                            cursor_to_enova.executemany("""INSERT INTO """+table_name_duplicate_for_insert+""" 
                                (Pracownik, Data, Godzina, Typ, Operacja, CzytnikRCP) 
                                VALUES (%s, %d, %d, %d, %d, %d)""", buff_tuple)
                            connection_to_enova.commit()
                        ##
                    except:
                        errors = True
                        console.print(Panel("error insert to table"), style=rich_error)
                        console.print(Panel(traceback.format_exc()), style=rich_error)
                        y_n = Prompt.ask("continue?", choices=['y', 'n'])
                        if y_n == 'n':
                            return False
                #end step 5#
                ############
            #end if not empty#

            #if user pressed continue#
            if errors == True:
                errors = False
            #
            finish_rows.clear()
            finish_rows_duplicate.clear()
            while_for_import += 1
            bar.next()
            #
        #while in import end#
        #####################
        bar.finish()
        return True
        
    #end import data from viso to enova#
    ####################################

    #####################################
    #import data from bojkowska to enova#
    def import_data_from_bojkowska_to_enova():
        ##
        buff_str = ''
        list_for_data_from_bojkowska = [] 

        #check datetime#
        date1 = Prompt.ask("date 1 (YYYY-MM-DD)")#"2022-03-01"
        date2 = Prompt.ask("date 2 (YYYY-MM-DD)")#"2022-03-31"
        date1_datetime = datetime.strptime(date1, '%Y-%m-%d')
        date2_datetime = datetime.strptime(date2, '%Y-%m-%d')
        #
        while date1_datetime.month != date2_datetime.month:
            cls()
            console.print("error")
            date1 = Prompt.ask("date 1 (YYYY-MM-DD)")#"2022-03-01"
            date2 = Prompt.ask("date 2 (YYYY-MM-DD)")#"2022-03-31"
            date1_datetime = datetime.strptime(date1, '%Y-%m-%d')
            date2_datetime = datetime.strptime(date2, '%Y-%m-%d')

        bar = ShadyBar('', max=5)
        
        #open text file#
        try:
            logfile = open(config.get("LOG_FILES", "filename from bojkowska"), 'r') 
        except:
            console.print(Panel("error open file"), style=rich_error)
            console.print(Panel(traceback.format_exc()), style=rich_error)
            return False

        ########
        #step 1# 
        
        ##################################
        #its step get list from text file#
        ##################################


        list_from_file = [] 
        ##
        try:
            #write text file to list#
            for i_for_file in logfile:
                list_from_file.append(i_for_file)

            #delete spaces in end rows#
            i_in_while = 0
            while i_in_while < len(list_from_file):
                #row_for_data_from_bojkowska.clear()
                
                list_from_file[i_in_while] = list_from_file[i_in_while].strip()
                #
                row_for_data_from_bojkowska = []
                for symbol in list_from_file[i_in_while]:
                    #
                    if symbol != ';' and symbol != 'T' and symbol != '/':
                        buff_str += symbol
                    #
                    else:
                        row_for_data_from_bojkowska.append(buff_str)
                        buff_str = ''
                #
                list_for_data_from_bojkowska.append(row_for_data_from_bojkowska)
                del row_for_data_from_bojkowska
                i_in_while += 1
        except:
            console.print(Panel("step 1 error\nread from file error"), style=rich_error)
            console.print(Panel(traceback.format_exc()), style=rich_error)
            return False
        bar.next()
        #end step 1#
        ############

        ########
        #step 2#

        ################
        #create id list#
        ################
        buff_id_list = []

        try:
            i_in_while = 0
            while i_in_while < len(list_for_data_from_bojkowska):
                buff_id_list.append(list_for_data_from_bojkowska[i_in_while][2])
                i_in_while += 1
        except:
            console.print(Panel("step 2 error\n create id list error"), style=rich_error)
            console.print(Panel(traceback.format_exc()), style=rich_error)
            return False

        ##############################
        #delete douplicate in id_list#

        buff_id_list = set(buff_id_list)
        id_list = list(buff_id_list)
        #end delete doublicate in id_list#
        ##################################

        bar.next()
        #end step 2#
        ############
        
        ##########
        #step 2.1#
        
        ################################
        #create dictionary RCP <---> ID#
        ################################
        id_list_dictionary = {}
        ####
        try:
            if test_mode == False:
                ##
                i_in_while = 0
                while i_in_while < len(id_list):
                    cursor_to_enova.execute("""
                            SELECT KartyRCP.Pracownik 
                            FROM KartyRCP
                            WHERE
                            KartyRCP.Numer =  
                            '"""+id_list[i_in_while]+"'") 
                    row = cursor_to_enova.fetchall()
                    ##
                    if row:
                        id_list_dictionary[id_list[i_in_while]] = row[0][0]
                    else:
                        del id_list[i_in_while]
                        continue
                    ##
                    i_in_while += 1
            ##
            else:
                i_in_while = 0
                while i_in_while < len(id_list):
                    id_list_dictionary[id_list[i_in_while]] = id_list[i_in_while]
                    i_in_while += 1
        except:
            console.print(Panel("step 2.1 error\ncreate dictionary RCP <---> ID error"), style=rich_error)
            console.print(Panel(traceback.format_exc()), style=rich_error)
            return False

        #end step 2.1#
        ##############

        ##########
        #step 2.2#

        ######################
        #delete dow if no RCP#
        ######################
        
        i_in_while = 0
        while i_in_while < len(list_for_data_from_bojkowska):
            if id_list_dictionary.get(list_for_data_from_bojkowska[i_in_while][2]) == None:
                del list_for_data_from_bojkowska[i_in_while]
                continue
            else:
                i_in_while += 1
        
        #end step 2.2#
        ##############

        ########
        #step 3#

        ##########################################
        #create datetime columns and summ columns#
        ##########################################
        try:
            i_in_while = 0
            while i_in_while < len(list_for_data_from_bojkowska):
                list_for_data_from_bojkowska[i_in_while][0] = datetime.strptime(list_for_data_from_bojkowska[i_in_while][0], "%Y-%m-%d")
                buff_datetime = datetime.strptime(list_for_data_from_bojkowska[i_in_while][1], "%H:%M:%S")
                list_for_data_from_bojkowska[i_in_while][1] = str(buff_datetime.hour * 60 + buff_datetime.minute)
                i_in_while += 1 
            ##
            buff_list_for_data_from_bojkowska = []
            i_in_while = 0
        except:
            console.print(Panel("error step 3\n create datetime columns and summ columns error"))
            console.print(Panel(traceback.format_exc()))
            return False

        #################
        #datetime sample#
        #################
        try:
            while i_in_while < len(list_for_data_from_bojkowska):
                #
                if list_for_data_from_bojkowska[i_in_while][0] >= date1_datetime and list_for_data_from_bojkowska[i_in_while][0] <= date2_datetime:
                    buff_list_for_data_from_bojkowska.append(list_for_data_from_bojkowska[i_in_while])
                i_in_while += 1
            ##
            list_for_data_from_bojkowska = buff_list_for_data_from_bojkowska
            del buff_list_for_data_from_bojkowska
        except:
            console.print(Panel("error step 3.1\n datetime sample error"), style=rich_error)
            console.print(Panel(traceback.format_exc()), style=rich_error)
            return False

        ##########################################
        #sorting list by id and delete duplicates#
        ##########################################
        buff_list_for_sorted_data = []

        i_in_while = 0
        try:
            while i_in_while < len(id_list):
                #
                block = []
                j_in_while = 0
                while j_in_while < len(list_for_data_from_bojkowska):
                    if id_list[i_in_while] == list_for_data_from_bojkowska[j_in_while][2]:
                        block.append(list_for_data_from_bojkowska[j_in_while])
                    j_in_while += 1
                #
                j_in_while = 1
                while j_in_while < len(block)-1:
                    if block[j_in_while][0] == block[j_in_while+1][0]:
                        del block[j_in_while]
                    else:
                        j_in_while += 2
                #
                j_in_while = 0
                while j_in_while < len(block):
                    buff_list_for_sorted_data.append(block[j_in_while])
                    j_in_while += 1
                #
                i_in_while += 1
            ##
            list_for_data_from_bojkowska = buff_list_for_sorted_data
            del buff_list_for_sorted_data
        except:
            console.print(Panel("error step 3.2\nerror sorting list by id and delete duplicate"), style=rich_error)
            console.print(Panel(traceback.format_exc()), style=rich_error)
            return False
        ##
        bar.next()
        #end step 3#
        ############

        ########
        #step 4#
        
        ####################
        #create finish list#
        ####################
        finish_list = []
        finish_row = []
        #
        finish_list_duplicate = []
        finish_row_duplicate = []
        #
        bool_for_typ = False
        #
        i_in_while = 0
        while i_in_while < len(list_for_data_from_bojkowska):
            try:
                typ =  0
                operacja = 0
                #
                if bool_for_typ == False:
                    typ = 1
                    operacja = 0
                    bool_for_typ = True
                else:
                    typ = 2
                    operacja = 16
                    bool_for_typ = False
                #
                finish_row = [id_list_dictionary.get(list_for_data_from_bojkowska[i_in_while][2]),
                        list_for_data_from_bojkowska[i_in_while][0],
                        list_for_data_from_bojkowska[i_in_while][1],
                        typ,
                        operacja,
                        0,
                        0,
                        0,
                        11]
                #
                finish_list.append(tuple(finish_row))
                finish_row.clear()
                ##
                if test_mode == False:
                    finish_row_duplicate = [id_list_dictionary.get(list_for_data_from_bojkowska[i_in_while][2]),
                                    list_for_data_from_bojkowska[i_in_while][0],
                                    list_for_data_from_bojkowska[i_in_while][1],
                                    typ,
                                    operacja,
                                    11]
                    #
                    finish_list_duplicate.append(tuple(finish_row_duplicate))
                    finish_row_duplicate.clear()
                ##
                i_in_while += 1
            except:
                console.print(Panel("error create finish list"), style=rich_error)
                console.print(Panel(traceback.format_exc()), style=rich_error)
                return False
        #end while
        bar.next()
        #end step 4#
        ############

        ########
        #step 5#

        #################
        #insert to enova#
        #################
        buff_tuple = tuple(finish_list)
        ##
        try:
            cursor_to_enova.executemany("""INSERT INTO """+table_name_for_insert+""" 
                (Pracownik, Data, Godzina, Typ, Operacja, Zaimportowany, Stan, Zmodyfikowany, CzytnikRCP) 
                VALUES (%s, %d, %d, %d, %d, %d, %d, %d, %d)""", buff_tuple)
            connection_to_enova.commit()
            ##
            if test_mode == False:
                #duplicate#
                buff_tuple = tuple(finish_list_duplicate)
                cursor_to_enova.executemany("""INSERT INTO """+table_name_duplicate_for_insert+""" 
                    (Pracownik, Data, Godzina, Typ, Operacja, CzytnikRCP) 
                    VALUES (%s, %d, %d, %d, %d, %d)""", buff_tuple)
                connection_to_enova.commit()
        except:
            console.print(Panel("error insert to enova"), style=rich_error)    
            console.print(Panel(traceback.format_exc()), style=rich_error)
            return False

        ##
        bar.next()
        bar.finish() 
        return True
        #end step 5#
        ############

    #end import data from bojkowska to enova#
    #########################################

    #mssql end#
    ###########

    #######
    #menus#

    ###########
    #main menu#
    bool_to_main_menu_while = False
    def main_menu():
        ##
        global bool_to_main_menu_while
        bool_to_main_menu_while = True
        #
        global connection_to_viso
        global bool_to_main_while
        global database_num
        ##
        menu_counter: int = 0
        ##
        while bool_to_main_menu_while:
            ##
            if test_mode == True:
                console.print(Panel("WARNING\nTEST MODE IS ON"), style=rich_error)
            console.print(Panel("#"+database_name_for_enova+"#"), style=rich_sky_blue)
            ##
            console.print(Panel(""
                +"1. import data from bojkowska to enova\n"
                +"2. import data from viso to enova\n"
                +"3. select db\n"
                +"0. exit programm",
                expand=True, box=box.ROUNDED), style=rich_ok)
            ##
            menu_count = IntPrompt.ask("enter number", choices=['0', '1', '2', '3'], show_choices=False)
            ##
            if menu_count == 1:
                if import_data_from_bojkowska_to_enova():
                    console.print(Panel("SUCCESFUL"))
                    press_any_key()
                else:
                    console.print(Panel("!!!error import data!!!"))
                    press_any_key()
            #
            elif menu_count == 2:
                if import_data_from_viso_to_enova():
                    console.print(Panel("SUCCESFUL"))
                    press_any_key()
                else:
                    console.print(Panel("!!!error import data!!!"))
                    press_any_key()
            #
            elif menu_count == 3:
                cls()
                console.print(Panel(
                "1. "+config.get("ENOVA", "database_1")+"\n"
                +"2. "+config.get("ENOVA", "database_2")+"\n"
                +"3. "+config.get("ENOVA", "database_3")+"\n"
                +"4. "+config.get("ENOVA", "database_4")
                , expand=True, box=box.ROUNDED), style=rich_ok)
                database_num = IntPrompt.ask("enter number", choices=['1', '2', '3', '4'], show_choices=False)
                bool_to_main_menu_while = False
            #
            elif menu_count == 0:
                bool_to_main_menu_while = False
                bool_to_main_while = False
            ##
            cls()

    #main menu end#
    ###############

    #menus end#
    ###########

    ######
    #main#

    if __name__ == "__main__":
        main_menu()
        connection_to_enova.close()
        connection_to_viso.close()

    #main end#
    ##########
