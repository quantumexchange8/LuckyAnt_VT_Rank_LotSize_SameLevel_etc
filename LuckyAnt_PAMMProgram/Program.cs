
using System;
using System.Net;
using System.Net.Http;

//using Newtonsoft.Json;
using System.IO;
using System.Text;
using Newtonsoft.Json.Linq;
using System.Data;
using System.Text.Json;
using System.Collections.Generic;
using System.Text.Json.Serialization;

using System.Threading.Tasks;
using System.Diagnostics;
using MetaQuotes.MT5CommonAPI;
using MetaQuotes.MT5ManagerAPI;
using System.Collections.Generic;
using System.Linq;

using MySql.Data.MySqlClient;
using MySqlX.XDevAPI.Relational;

using Telegram.Bot;
using Telegram.Bot.Types;

namespace LuckyAnt
{
    internal class Program
    {
        /// <summary>
        ///  --------------  LIVE DATABASE  --- LIVE DATABASE -- LIVE DATABASE --- CAUTION
        /// </summary>
        private static DateTime default_time = new(2020, 1, 1);
        //private static string conn = "server = 174.138.30.5; uid = wpadmin; pwd = pB3$81Ef5DD; database = luckyant-mt5; port = 3306;";
        private static string ipadd_url = "http://103.21.90.87:8080/serverapi/pamm/strategy-summary?";
        private static string conn = "server = 68.183.177.155; uid = ctadmin; pwd = CTadmin!123; database = mt5-crm; port = 3306;";
        private static string db_name = "mt5-crm";
        private static string mode_type = "demo"; // live
        private static long chatId = -4034138212;
        private static string telegramApiToken = "6740313709:AAEILXwPzjUtEJH343edziI_wuQqbTPQ8ew";
        private static string title_name = "LuckyAnt-VT PAMM Program";
        //1000(1 second), 60,000 (1 minutes), 
        private static int expired_second = 60000 * 15; // 15 minutes
        //private static long lucky_ant_id = 2; // live - 7 
        private static long lucky_ant_id = 7; // live - 7 
        private static long bonus_precent_given = 0;
        private static long rewards_precent_given = 0;    

        static async Task Main()
        {
            Console.WriteLine("Current database:" + conn);
            Console.WriteLine("================================================================================");
            DateTime currentDate = DateTime.Now;
            currentDate = new DateTime(currentDate.Year, currentDate.Month, currentDate.Day, 8, 0, 0);

            DateTime YtdDate = currentDate.AddDays(-1);
            string input = await AwaitConsoleReadLine(1000);

            //string input = "";
            //do
            //{
            if (input == "Y" || input == "y" || input == null)
            {                
                bool upgrade_rank_progress = true;
                bool sponsor_bonus_progress = true;
                
                bool retrieve_data_apilink = true;
                bool calculate_bonus_progress = true;

                // ranking progress
                if (upgrade_rank_progress == true)
                {
                    proceed_ranking();
                    proceed_manual_ranking();
                }            
                
                if(sponsor_bonus_progress == true || calculate_bonus_progress == true)
                {
                    bonus_rewards_percent(ref bonus_precent_given, ref rewards_precent_given);
                }

                // sponsor bonus progress
                if (sponsor_bonus_progress == true)
                {
                    //YtdDate = new DateTime(2024, 6, 20, 8, 0, 0);
                    int sponsor_level = 20;
                    double sponsor_pct = ((double) 5)/ 100;
                    proceed_sponsor_bonus(YtdDate, sponsor_level, sponsor_pct);
                }
                
                // retrieve pamm data via api and insert to trade histories
                if(retrieve_data_apilink == true) {     
                    //YtdDate = new DateTime(2024, 6, 25, 8, 0, 0);
                    await api_url_async(YtdDate);   }

                if (calculate_bonus_progress == true)
                {
                    // waiting - need to identify which users not under lucky Ant
                    var taskStopwatch = Stopwatch.StartNew();
                    Console.WriteLine(" ");
                    Console.WriteLine("Lot Size Rebate & Same Level program started...");

                    try
                    {
                        DateTime YTDDateTime = currentDate.AddDays(-1);
                        DateTime YTD_start = new DateTime(YTDDateTime.Year, YTDDateTime.Month, YTDDateTime.Day, 0, 0, 0);
                        DateTime YTD_end = new DateTime(YTDDateTime.Year, YTDDateTime.Month, YTDDateTime.Day, 23, 59, 59);

                        Console.WriteLine($"Today YTDDateTime: {currentDate.ToString("yyyy-MM-dd HH:mm:ss")}");
                        Console.WriteLine($"Ytd YTD_start: {YTD_start.ToString("yyyy-MM-dd HH:mm:ss")} - YTD_end: {YTD_end.ToString("yyyy-MM-dd HH:mm:ss")}");

                        if (bonus_precent_given > 0 && rewards_precent_given > 0)
                        {
                            //update_rebate_not_underLA(YTD_start, YTD_end);
                            //update_rebate_if_demofund_or_master(YTD_start, YTD_end);
                            
                            proceed_rebate_calculation(YTD_start, YTD_end);
                            proceed_summary_rebate(currentDate);
                            //proceed_same_level_rewards_personal(currentDate); //currentDate
                            //proceed_same_level_rewards_group(currentDate);
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"An exception occurred: {ex}");
                    }
                    
                    taskStopwatch.Stop();
                    Console.WriteLine("");
                    Console.WriteLine($"Task calculate_bonus_progress completed in {taskStopwatch.Elapsed.TotalSeconds} seconds ({taskStopwatch.Elapsed.TotalMinutes})");
                }
            }
            else if (input == "N" || input == "n")
            {
                Console.WriteLine("operation cancelled!");
                return;
            }
            else
            {
                Console.WriteLine("Invalid input! Please try again!");
            }

            //} while (input != "Y" && input != "y" && input != "N" && input != "n" && input != "");
            //return;
        }

        private static async Task api_url_async(DateTime datetime_day) //Task<JArray>
        {
            try
            {
                List<ulong> masterid_List = new List<ulong>();
                using (MySqlConnection sql_conn = new MySqlConnection(conn))
                {
                    sql_conn.Open();
                    string selectQuery = "SELECT id FROM masters where deleted_at is null and type = 'PAMM' and status = 'Active' and signal_status = 1;";
                    MySqlCommand select_cmd = new MySqlCommand(selectQuery, sql_conn);
                    MySqlDataReader reader = select_cmd.ExecuteReader();
                
                    if (reader.HasRows)
                    {
                        while(reader.Read())
                        {
                            ulong masterid=reader.GetUInt64(0);
                            if(masterid > 0) { masterid_List.Add(masterid);   }
                        }
                    }
                }        
                await GetApiResponseAsync_N_insertDB(datetime_day, masterid_List);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"An exception occurred: {ex}");
            }
        }

        private static async Task GetApiResponseAsync_N_insertDB( DateTime datetime_day1, List<ulong> masterid_List) //<ApiResponse>
        {
            try
            {
                using (HttpClient httpClient = new HttpClient())
                {
                    foreach (var masterid0 in masterid_List)
                    {
                        Console.WriteLine(" ");
                        ulong master_id0 = (ulong) masterid0;
                        Console.WriteLine($"url {$"{ipadd_url}day={datetime_day1.ToString("yyyy-MM-dd")}&id={masterid0}"}");

                        string api_url = $"{ipadd_url}day={datetime_day1.ToString("yyyy-MM-dd")}&id={masterid0}";
                        Uri apiUrl = new Uri(api_url);
                        //httpClient.Timeout = TimeSpan.FromSeconds(5); // Set a timeout
                        HttpResponseMessage response = await httpClient.GetAsync(api_url);

                        if (response.IsSuccessStatusCode)
                        {
                            string responseBody = await response.Content.ReadAsStringAsync();
                            ApiResponse apiResponse = JsonSerializer.Deserialize<ApiResponse>(responseBody);
                            
                            if (apiResponse != null)
                            {
                                Console.WriteLine($"Day: {apiResponse.Day ?? "null"}");
                                Console.WriteLine($"ID: {apiResponse.Id?.ToString() ?? "null"}");
                                Console.WriteLine($"Lot: {apiResponse.Lot?.ToString() ?? "null"}");
                                Console.WriteLine($"Profit and Loss: {apiResponse.ProfitAndLoss?.ToString() ?? "null"}");
                                Console.WriteLine($"Number of Trade: {apiResponse.NumberOfTrade?.ToString() ?? "null"}");

                                string datetime_day = apiResponse.Day ?? default_time.ToString("yyyy-MM-dd");
                                long master_id =  apiResponse.Id ?? 0;
                                double master_lot =  apiResponse.Lot ?? 0.0;
                                double master_pnl =  apiResponse.ProfitAndLoss ?? 0.0;
                                long master_trades =  apiResponse.NumberOfTrade ?? 0;

                                if (apiResponse.Subscriptions != null)
                                {
                                    foreach (var subscription in apiResponse.Subscriptions)
                                    {
                                        Console.WriteLine($"Subscription ID: {subscription.Id?.ToString() ?? "null"}, " +
                                                        $"User ID: {subscription.UserId?.ToString() ?? "null"}, " +
                                                        $"Lot: {subscription.Lot?.ToString() ?? "null"}, " +
                                                        $"Profit and Loss: {subscription.ProfitAndLoss?.ToString() ?? "null"}");

                                        long subs_id = subscription.Id ?? 0;
                                        long subs_userid = subscription.UserId ?? 0;

                                        double subs_lot = subscription.Lot ?? 0.00;
                                        double subs_pnl = subscription.ProfitAndLoss ?? 0.00;

                                        using (MySqlConnection sql_conn = new MySqlConnection(conn))
                                        {
                                            long subs_meta_login = 0;

                                            sql_conn.Open(); // Open the connection
                                            string login_sqlstr =  $"SELECT meta_login FROM subscriptions where deleted_at is null and user_id = {subs_userid} and id = {subs_id};";
                                            MySqlCommand select_cmd = new MySqlCommand(login_sqlstr, sql_conn);
                                            object result = select_cmd.ExecuteScalar();
                                            if (result != null) {   subs_meta_login = Convert.ToInt64(result);  }

                                            if(subs_meta_login > 0)
                                            {
                                                string sqlstr = $"INSERT INTO trade_histories( master_id, master_lot, master_pnl, master_num_trades, subscription_id, user_id, meta_login,"+
                                                                $"volume, time_close, trade_profit, rebate_status, created_at ) VALUES ( " +
                                                                $"{master_id}, {master_lot}, Round({master_pnl},4), Round({master_trades},4), {subs_id}, {subs_userid}, {subs_meta_login}, "+
                                                                $"Round({subs_lot},4), '{datetime_day}', Round({subs_pnl},4), 'Pending', '{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")}'); ";
                                                //Console.WriteLine($"insert_cmd sqlstr: {sqlstr}");
                                                MySqlCommand insert_cmd = new MySqlCommand(sqlstr, sql_conn);
                                                insert_cmd.ExecuteScalar();
                                            }
                                        }
                                    }
                                }
                                else{   Console.WriteLine("Subscriptions: null");   }
                            }
                            else{   Console.WriteLine("ApiResponse is null.");  }
                        }
                    }
                }
            }
            catch (HttpRequestException httpEx) {   Console.WriteLine($"HTTP Request Exception: {httpEx.Message}"); }
            catch (TaskCanceledException tcEx)  {   Console.WriteLine($"Request Timed Out: {tcEx.Message}");    }
            catch (Exception ex)    {   Console.WriteLine($"An exception occurred: {ex}");  }
        }

        public class Subscription
        {
            [JsonPropertyName("id")]
            public int? Id { get; set; }

            [JsonPropertyName("user_id")]
            public int? UserId { get; set; }

            [JsonPropertyName("lot")]
            public double? Lot { get; set; }

            [JsonPropertyName("profit_and_loss")]
            public double? ProfitAndLoss { get; set; }
        }

        public class ApiResponse
        {
            [JsonPropertyName("day")]
            public string? Day { get; set; }

            [JsonPropertyName("id")]
            public int? Id { get; set; }

            [JsonPropertyName("lot")]
            public double? Lot { get; set; }

            [JsonPropertyName("profit_and_loss")]
            public double? ProfitAndLoss { get; set; }

            [JsonPropertyName("number_of_trade")]
            public int? NumberOfTrade { get; set; }

            [JsonPropertyName("subscriptions")]
            public List<Subscription> Subscriptions { get; set; }
        }

        private static void retrieve_subscription_basedon_userid(long user_id, ref List<object[]> subs_data)
        {
            try
            {
                using (MySqlConnection sql_conn = new MySqlConnection(conn))
                {
                    sql_conn.Open(); // Open the connection
                    string sqlstr = $"SELECT id, COALESCE(cumulative_amount, 0) AS cumulative_amount, COALESCE(max_out_amount, 0) AS max_out_amount, subscription_number, meta_login "+
                                    $"FROM subscriptions where deleted_at is null and status = 'Active' and type = 'PAMM' "+ // 
                                    $"and COALESCE(cumulative_amount, 0) < COALESCE(max_out_amount, 0) and user_id = {user_id} ;";
                    
                    //Console.WriteLine($"retrieve_subscription_basedon_userid - sqlstr: {sqlstr} ");  
                    MySqlCommand select_cmd = new MySqlCommand(sqlstr, sql_conn);
                    MySqlDataReader reader = select_cmd.ExecuteReader();
                    while (reader.Read())
                    {   // sub_id, cumulative_amount, max_out_amount, quota, subscription_number, meta_login
                        double quota = reader.GetDouble(2) - reader.GetInt64(1);
                        object[] subscriptData = { reader.GetInt64(0), reader.GetDouble(1), reader.GetDouble(2), quota, reader.GetString(3), reader.GetInt64(4)}; //, reader.GetDouble(6), reader.GetDouble(7) };
                        subs_data.Add(subscriptData);    
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"An exception occurred: {ex}");
            }
        }

        private static void retrieve_bonus_e_wallet_data(long user_id, ref long bonus_wallet_id, ref double bonus_wallet_bal, ref long e_wallet_id, ref double e_wallet_bal )
        {
            try
            {
                using (MySqlConnection sql_conn = new MySqlConnection(conn))
                {
                    sql_conn.Open(); // Open the connection
                     
                    string bonus_wallet_sqlstr = $"select id, balance from wallets where deleted_at is null and type = 'bonus_wallet' and user_id = {user_id};";
                    //Console.WriteLine($"bonus_wallet_sqlstr: {bonus_wallet_sqlstr}");
                    MySqlCommand bonus_wallet_cmd = new MySqlCommand(bonus_wallet_sqlstr, sql_conn);
                    MySqlDataReader result0 = bonus_wallet_cmd.ExecuteReader();
                    while (result0.Read())
                    {
                        bonus_wallet_id = result0.GetInt64(0);
                        bonus_wallet_bal = result0.GetDouble(1);
                    }
                    result0.Close();

                    string e_wallet_sqlstr = $"select id, balance from wallets where deleted_at is null and type = 'e_wallet' and user_id = {user_id};";
                    //Console.WriteLine($"e_wallet_sqlstr: {e_wallet_sqlstr}");
                    MySqlCommand e_wallet_cmd = new MySqlCommand(e_wallet_sqlstr, sql_conn);
                    MySqlDataReader result1 = e_wallet_cmd.ExecuteReader();
                    while (result1.Read())
                    {
                        e_wallet_id = result1.GetInt64(0);
                        e_wallet_bal = result1.GetDouble(1);
                    }
                    result1.Close();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"An exception occurred: {ex}");
            }
        }
    
        private static void bonus_rewards_percent(ref long bonus_precent, ref long rewards_precent)
        {
            using (MySqlConnection sql_conn = new MySqlConnection(conn))
            {
                sql_conn.Open(); // Open the connection
                try
                {
                    string bonus_given_sqlstr = $"select value from settings where deleted_at is null and slug ='bonus-percent-given';";
                    Console.WriteLine($"bonus_given_sqlstr: {bonus_given_sqlstr}");

                    MySqlCommand bonus_given_cmd = new MySqlCommand(bonus_given_sqlstr, sql_conn);
                    object result0 = bonus_given_cmd.ExecuteScalar();
                    if (result0 != null) { bonus_precent = Convert.ToInt64(result0); }

                    string rewards_given_sqlstr = $"select value from settings where deleted_at is null and slug ='rewards-percent-given';";
                    Console.WriteLine($"rewards_given_sqlstr: {rewards_given_sqlstr}");

                    MySqlCommand rewards_given_cmd = new MySqlCommand(rewards_given_sqlstr, sql_conn);
                    object result1 = rewards_given_cmd.ExecuteScalar();
                    if (result1 != null) { rewards_precent = Convert.ToInt64(result1); }
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Error sending message: " + ex.Message);
                }
            }
        }

        private static void insert_to_sponsor_bonus(long upline_id, long upline_meta_login, long upline_subscription_id, 
                                                    long sub_userid, long meta_login, long sub_id, string sub_number,
                                                    double meta_bal, double bonus_pct, double total_bonus, double final_amt, string hier_remarks, long hier_lvl)
        {
            Console.WriteLine($"insert_to_sponsor_bonus ");
            long  bonus_wallet_id = 0;  double bonus_wallet_oldbal = 0;
            long  e_wallet_id = 0;      double e_wallet_oldbal = 0;
            
            retrieve_bonus_e_wallet_data( upline_id, ref bonus_wallet_id, ref bonus_wallet_oldbal, ref e_wallet_id, ref e_wallet_oldbal );
            
            double bonus_precent_decimal = (double)bonus_precent_given / 100;
            double rewards_precent_decimal = (double)rewards_precent_given / 100;

            // rebate divide 80% and 20%
            double rebate_major = Math.Round(final_amt * bonus_precent_decimal, 4); // Bonus Wallet
            double rebate_minor = Math.Round(final_amt * rewards_precent_decimal, 4); // E-wallets

            double new_bonus_wallet = bonus_wallet_oldbal + rebate_major;
            double new_e_wallet = e_wallet_oldbal + rebate_minor;
            
            Console.WriteLine($" ");
            using (MySqlConnection sql_conn = new MySqlConnection(conn))
            {
                sql_conn.Open(); // Open the connection

                 string sqlstr = $"INSERT INTO trade_sponsor_bonus(upline_id, upline_meta_login, upline_subscription_id, "+
                                 $"subs_user_id, meta_login, subscription_id, subscription_number," +
                                 $"meta_balance, bonus_percentage, total_bonus, final_bonus, hier_remarks, hier_levels, created_at, updated_at)" +
                                 $"VALUES ({upline_id}, {upline_meta_login}, {upline_subscription_id}, "+
                                 $"{sub_userid}, {meta_login}, {sub_id}, '{sub_number}', " +
                                 $"Round({meta_bal},4), {bonus_pct}, Round({total_bonus},4), Round({final_amt},4), '{hier_remarks}', {hier_lvl}, " +
                                 $"'{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")}', '{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")}');";

                //Console.WriteLine($"insert_cmd sqlstr: {sqlstr}");
                MySqlCommand insert_cmd = new MySqlCommand(sqlstr, sql_conn);
                insert_cmd.ExecuteScalar();
            }

            //Console.WriteLine($" ");
            string remarks_major = $"SponsorBonus(bonus_wallet) => ${Math.Round(final_amt, 4)} * {bonus_precent_given}% = ${rebate_major}";
            insert_to_wallet_walletlog_transaction(0, upline_id, upline_subscription_id, bonus_wallet_id, bonus_wallet_oldbal, new_bonus_wallet, "bonus", "SponsorBonus", rebate_major, remarks_major );  

            //Console.WriteLine($" ");
            string remarks_minor = $"SponsorBonus(e_wallet) => ${Math.Round(final_amt, 4)} * {rewards_precent_decimal}% = ${rebate_minor}";    
            insert_to_wallet_walletlog_transaction(1, upline_id, upline_subscription_id, e_wallet_id, e_wallet_oldbal, new_e_wallet, "bonus", "SponsorBonus", rebate_minor,  remarks_minor );    
            
        }

        private static void insert_to_wallet_walletlog_transaction(long type_id, long upline_id, long subs_id, long wallet_id, double wallet_oldbal, double wallet_newbal, string category, string purpose,
                                                        double bonus_amt, string walletlog_remarks )
        {
            //var taskStopwatch = Stopwatch.StartNew();
            Console.WriteLine($"insert_to_wallet_walletlog ... {type_id}");
            try
            {
                using (MySqlConnection sql_conn = new MySqlConnection(conn))
                {
                    sql_conn.Open(); // Open the connection
                    if(type_id == 0)
                    {
                        string walletmajor_sqlstr = "INSERT INTO wallet_logs (user_id, wallet_id, old_balance, new_balance, wallet_type, category, purpose, amount, remark, created_at) " +
                                                    $"VALUES ({upline_id}, {wallet_id}, ROUND({wallet_oldbal},4), ROUND({wallet_newbal},4), 'bonus_wallet', '{category}', '{purpose}', " +
                                                    $"ROUND({bonus_amt},4), '{walletlog_remarks}', '{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")}' );";
                        Console.WriteLine($"walletmajor_sqlstr: {walletmajor_sqlstr}");
                        MySqlCommand b_wallet_insert_cmd = new MySqlCommand(walletmajor_sqlstr, sql_conn);
                        b_wallet_insert_cmd.ExecuteScalar();

                        string walletmajor_update = $"update wallets set balance = ROUND( ( COALESCE(balance, 0) + {bonus_amt}),4), updated_at = '{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")}' " +
                                                    $"where deleted_at is null and user_id = {upline_id} and type = 'bonus_wallet' and id > 0; ";
                        Console.WriteLine($"walletmajor_update: {walletmajor_update}");
                        MySqlCommand b_wallet_update_cmd = new MySqlCommand(walletmajor_update, sql_conn);
                        b_wallet_update_cmd.ExecuteScalar();

                        string max_major_update = $"update subscriptions set cumulative_amount = ROUND( ( COALESCE(cumulative_amount, 0) + {bonus_amt}),4), updated_at = '{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")}' " +
                                                    $"where deleted_at is null and id = {subs_id} and user_id = {upline_id} and id > 0; ";
                        Console.WriteLine($"max_major_update: {max_major_update}");
                        MySqlCommand max_major_upd_cmd = new MySqlCommand(max_major_update, sql_conn);
                        max_major_upd_cmd.ExecuteScalar();

                        string b_walletlog_id_str = $"SELECT id FROM wallet_logs where category = '{category}' and wallet_type = 'bonus_wallet' and purpose = '{purpose}' and new_balance = ROUND({wallet_newbal},4) " +
                                         $"and user_id = {upline_id} and DATE(created_at) = DATE('{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")}') order by created_at desc limit 1;";
                        Console.WriteLine($"b_walletlog_id_str: {b_walletlog_id_str}");
                        MySqlCommand b_select_cmd = new MySqlCommand(b_walletlog_id_str, sql_conn);
                        long b_walletlog_id = Convert.ToInt64(b_select_cmd.ExecuteScalar());

                        string major_b_transaction =  $"INSERT INTO transactions (user_id, category, transaction_type, fund_type, to_wallet_id, amount, transaction_amount, new_wallet_amount, status, comment, created_at)"+
                                                    $"VALUES ( {upline_id}, 'wallet', '{purpose}', 'RealFund', {wallet_id}, ROUND({bonus_amt},4), ROUND({bonus_amt},4), ROUND({wallet_newbal},4), " +
                                                    $"'Success', '{b_walletlog_id}', '{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")}'); ";
                        Console.WriteLine($"major_b_transaction: {major_b_transaction}");
                        MySqlCommand b_insert_transaction_cmd = new MySqlCommand(major_b_transaction, sql_conn);
                        b_insert_transaction_cmd.ExecuteScalar();
                    }
                    if(type_id == 1)
                    {
                        string walletminor_sqlstr = "INSERT INTO wallet_logs (user_id, wallet_id, old_balance, new_balance, wallet_type, category, purpose, amount, remark, created_at) " +
                                                    $"VALUES ({upline_id}, {wallet_id}, ROUND({wallet_oldbal},4), ROUND({wallet_newbal},4), 'e_wallet', '{category}', '{purpose}', "+
                                                    $"ROUND({bonus_amt},4), '{walletlog_remarks}', '{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")}' );";
                        Console.WriteLine($"walletminor_sqlstr: {walletminor_sqlstr}");
                        MySqlCommand e_wallet_insert_cmd = new MySqlCommand(walletminor_sqlstr, sql_conn);
                        e_wallet_insert_cmd.ExecuteScalar();

                        string walletminor_update = $"update wallets set balance = ROUND( ( COALESCE(balance, 0)  + {bonus_amt}),4), updated_at = '{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")}' " +
                                                    $"where deleted_at is null and user_id = {upline_id} and type = 'e_wallet' and id > 0; ";
                        Console.WriteLine($"walletminor_update: {walletminor_update}");
                        MySqlCommand e_wallet_update_cmd = new MySqlCommand(walletminor_update, sql_conn);
                        e_wallet_update_cmd.ExecuteScalar();    

                        string max_minor_update = $"update subscriptions set cumulative_amount = ROUND( (COALESCE(cumulative_amount, 0) + {bonus_amt}),4), updated_at = '{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")}' " +
                                                    $"where deleted_at is null and id = {subs_id} and user_id = {upline_id} and id > 0; ";
                        Console.WriteLine($"max_minor_update: {max_minor_update}");
                        MySqlCommand max_minor_upd_cmd = new MySqlCommand(max_minor_update, sql_conn);
                        max_minor_upd_cmd.ExecuteScalar();

                        string e_walletlog_id_str = $"SELECT id FROM wallet_logs where category = '{category}' and wallet_type = 'e_wallet' and purpose = '{purpose}' and new_balance = ROUND({wallet_newbal},4) " +
                                         $"and user_id = {upline_id} and DATE(created_at) = DATE('{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")}') order by created_at desc limit 1;";
                        Console.WriteLine($"b_walletlog_id_str: {e_walletlog_id_str}");
                        MySqlCommand b_select_cmd = new MySqlCommand(e_walletlog_id_str, sql_conn);
                        long e_walletlog_id = Convert.ToInt64(b_select_cmd.ExecuteScalar());

                        string minor_e_transaction =  $"INSERT INTO transactions (user_id, category, transaction_type, fund_type, to_wallet_id, amount, transaction_amount, new_wallet_amount, status, comment, created_at)"+
                                                    $"VALUES ( {upline_id}, 'wallet', '{purpose}', 'RealFund', {wallet_id}, ROUND({bonus_amt},4), ROUND({bonus_amt},4), ROUND({wallet_newbal},4), " +
                                                    $"'Success', '{e_walletlog_id}', '{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")}'); ";
                        Console.WriteLine($"minor_e_transaction: {minor_e_transaction}");
                        MySqlCommand b_insert_transaction_cmd = new MySqlCommand(minor_e_transaction, sql_conn);
                        b_insert_transaction_cmd.ExecuteScalar();
                    }   
                } 
            }
            catch (Exception ex)
            {
                Console.WriteLine($"An exception occurred: {ex}");
            }

            /* taskStopwatch.Stop();
            Console.WriteLine("");
            Console.WriteLine($"Task sponsor_bonus completed in {taskStopwatch.Elapsed.TotalSeconds} seconds ({taskStopwatch.Elapsed.TotalMinutes})"); */
        }
        
        private static void proceed_sponsor_bonus(DateTime YtdDate, int sponsor_level, double sponsor_pct)
        {
            var taskStopwatch = Stopwatch.StartNew();
            Console.WriteLine("proceed_sponsor_bonus ... ");
            try
            {
                List<object[]> new_subscription_List = new List<object[]>();
                using (MySqlConnection sql_conn = new MySqlConnection(conn))
                {
                    sql_conn.Open(); // Open the connection
                    string sqlstr = $"SELECT user_id, meta_login, meta_balance, DATE(approval_date) as dt_time, id, subscription_number "+ 
                                    $"FROM subscriptions where deleted_at is null and status = 'Active' and approval_date is not null "+
                                    $"and type = 'PAMM' and DATE(approval_date) = DATE('{YtdDate.ToString("yyyy-MM-dd")}')  "+ 
                                    $"and id not in (select subscription_id from trade_sponsor_bonus where deleted_at is null) ;";

                    //Console.WriteLine($"proceed_sponsor_bonus sqlstr: {sqlstr}");
                    MySqlCommand select_cmd = new MySqlCommand(sqlstr, sql_conn);
                    MySqlDataReader reader = select_cmd.ExecuteReader();
                    while (reader.Read())
                    {                               // user_id, meta_login, meta_balance, dt_time
                        object[] subscriptionData = { reader.GetInt64(0), reader.GetInt64(1), reader.GetDouble(2), reader.GetDateTime(3), reader.GetInt64(4), reader.GetString(5)}; //, reader.GetDouble(6), reader.GetDouble(7) };
                        new_subscription_List.Add(subscriptionData);    
                    }
                }

                foreach (var new_subs in new_subscription_List)
                {
                    Console.WriteLine($" ");
                    Console.WriteLine($" ------------------------------------------------- new subs ");

                    // user_id, meta_login, meta_balance, dt_time
                    long subs_userid = (long)new_subs[0];
                    long meta_login = (long)new_subs[1];
                    double meta_balance = (double)new_subs[2];
                    DateTime dt_time = (DateTime)new_subs[3];
                    long subscription_id = (long)new_subs[4];
                    string subscription_number = (string)new_subs[5];

                    string subs_user_role = "";
                    string subs_hier_list = "";
                    
                    using (MySqlConnection sql_conn = new MySqlConnection(conn))
                    {
                        sql_conn.Open(); // Open the connection
                        string sqlstr = $"SELECT role, hierarchyList FROM users where deleted_at is null and status = 'Active' and id = {subs_userid} ;";
                        //Console.WriteLine($"select_cmd sqlstr: {sqlstr}");
                        MySqlCommand select_cmd = new MySqlCommand(sqlstr, sql_conn);
                        MySqlDataReader reader1 = select_cmd.ExecuteReader();
                        while (reader1.Read())
                        {
                            subs_user_role = reader1.GetString(0);  subs_hier_list = reader1.GetString(1);
                        }
                        reader1.Close();
                    }

                    Console.WriteLine($"subs_userid - {subs_userid} | meta_login: {meta_login} | meta_balance: {meta_balance} | subscription_id: {subscription_id} | sponsor_pct: {sponsor_pct} | sponsor_level: {sponsor_level}");
                    /* List<object[]> heir_lvl_List = new List<object[]>(); */
                    if(subs_user_role.Length > 0 && subs_hier_list.Length > 0 && subs_user_role == "user" && meta_balance > 0)
                    {
                        int level_count = 0;
                        double level_pct = sponsor_pct / sponsor_level;
                        double level_balance = meta_balance * level_pct;
                        string[] hierlistSplit = subs_hier_list.Split(new[] { '-' }, StringSplitOptions.RemoveEmptyEntries); 
                        for (int x = hierlistSplit.Length - 1; ( x >= 0 && level_count < sponsor_level); x--)
                        {
                            if (hierlistSplit[x] != "" )
                            {
                                level_count += 1;

                                long.TryParse(hierlistSplit[x], out long upline_id);
                                long upline_rank = 0; long upline_active = 0; string upline_role=""; 
                                retrieve_userid(upline_id, ref upline_rank, ref upline_active, ref upline_role);
                                    
                                if(upline_active > 0 && upline_role == "user")
                                {
                                    List<object[]> subs_data_list = new List<object[]>();
                                    retrieve_subscription_basedon_userid(upline_id, ref subs_data_list);

                                    //Console.WriteLine($"Count: {subs_data_list.Count} -- bal: {level_balance} -- pct: {level_pct} -- (level_count {level_count} <= sponsor_level {sponsor_level})");
                                    if(subs_data_list.Count > 0)
                                    {
                                        // sub_id, cumulative_amount, max_out_amount, quota, subscription_number, meta_login
                                        double left_bal = level_balance ;
                                        foreach (var subs_data in subs_data_list)
                                        {
                                            long upline_meta_login = (long) subs_data[5];
                                            long upline_sub_id = (long) subs_data[0];
                                            double quota = (double)subs_data[3];
                                            if(quota > 0 &&  left_bal > 0)
                                            {
                                                if(quota >= left_bal) {  
                                                    insert_to_sponsor_bonus(upline_id, upline_meta_login, upline_sub_id, subs_userid, meta_login, subscription_id,  
                                                                            subscription_number, meta_balance, level_pct, level_balance, left_bal, subs_hier_list, level_count);
                                                    left_bal = left_bal - level_balance;    //Console.WriteLine($" >= left_bal : {left_bal}");
                                                }
                                                else if (quota < left_bal){
                                                    insert_to_sponsor_bonus(upline_id, upline_meta_login, upline_sub_id, subs_userid, meta_login, subscription_id,  
                                                                            subscription_number, meta_balance, level_pct, level_balance, quota, subs_hier_list, level_count);
                                                    left_bal = left_bal - quota;    //Console.WriteLine($" < left_bal : {left_bal}");
                                                }
                                            }
                                        }
                                    }
                                }   
                            }
                        }
                    } 
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"An exception occurred: {ex}");
            }

            taskStopwatch.Stop();
            Console.WriteLine("");
            Console.WriteLine($"Task sponsor_bonus completed in {taskStopwatch.Elapsed.TotalSeconds} seconds ({taskStopwatch.Elapsed.TotalMinutes})");
        }

        private static string ConvertListToString(List<ulong> list)
        {
            // Use string.Join to concatenate the ulong values with commas
            return string.Join(",", list);
        }

        private static void update_rebate_if_demofund_or_master(DateTime YTD_start, DateTime YTD_end)
        {
            using (MySqlConnection sql_conn = new MySqlConnection(conn))
            {
                sql_conn.Open(); // Open the connection
                //SELECT id FROM users WHERE deleted_at is null and role='member' and status = 'Active' and id not in (7)  and ( top_leader_id is null or top_leader_id not in (7) )
                string sqlstr = $"UPDATE trade_histories SET rebate_status = 'Rejected', updated_at = '{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")}' WHERE " +
                                $"(rebate_status = 'Pending' or rebate_status = 'pending') and deleted_at is null and time_close <= '{YTD_end.ToString("yyyy-MM-dd HH:mm:ss")}' " +
                                $"and meta_login in (SELECT meta_login FROM trading_accounts where deleted_at is null and demo_fund > 0 ) AND id > 0; ";
                Console.WriteLine($"if_demofund sqlstr: {sqlstr}");
                MySqlCommand update_cmd = new MySqlCommand(sqlstr, sql_conn);
                update_cmd.ExecuteScalar();

                /* sqlstr = $"UPDATE trade_histories SET rebate_status = 'Rejected', updated_at = '{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")}' WHERE " +
                         $"(rebate_status = 'Pending' or rebate_status = 'pending') and deleted_at is null and time_close <= '{YTD_end.ToString("yyyy-MM-dd HH:mm:ss")}' " +
                         $"and meta_login in (select meta_login from masters where deleted_at is null and status = 'Active' ) AND id > 0; ";
                Console.WriteLine($"if_master sqlstr: {sqlstr}");
                MySqlCommand update_cmd1 = new MySqlCommand(sqlstr, sql_conn);
                update_cmd1.ExecuteScalar(); */

                Console.WriteLine($"ConnectionString: {sql_conn.ConnectionTimeout}");
            }
        }

        private static void update_rebate_not_underLA(DateTime YTD_start, DateTime YTD_end)
        {
            using (MySqlConnection sql_conn = new MySqlConnection(conn))
            {
                sql_conn.Open(); // Open the connection
                //SELECT id FROM users WHERE deleted_at is null and role='member' and status = 'Active' and id not in (7)  and ( top_leader_id is null or top_leader_id not in (7) )
                string sqlstr = $"UPDATE trade_histories SET rebate_status = 'Rejected', updated_at = '{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")}' WHERE " +
                                $"(rebate_status = 'Pending' or rebate_status = 'pending') and deleted_at is null and time_close <= '{YTD_end.ToString("yyyy-MM-dd HH:mm:ss")}' " +
                                $"and meta_login in (SELECT meta_login FROM trading_accounts where deleted_at is null and user_id in ( " +
                                $"SELECT id FROM users WHERE deleted_at is null and role='user' and status = 'Active' and id not in ({lucky_ant_id}) and ( top_leader_id is null or top_leader_id not in ({lucky_ant_id})) ) " +
                                $") AND id > 0; ";
                Console.WriteLine($"update_rebate_not_underLA sqlstr: {sqlstr}");
                MySqlCommand update_cmd = new MySqlCommand(sqlstr, sql_conn);
                update_cmd.ExecuteScalar();
                Console.WriteLine($"ConnectionString: {sql_conn.ConnectionTimeout}");
            }
        }
        
        private static void proceed_same_level_rewards_personal(DateTime TimeNow)
        {
            var taskStopwatch = Stopwatch.StartNew();
            Console.WriteLine("proceed_same_level_rewards_personal ... ");

            List<object[]> same_level_list = new List<object[]>();

            string sqlstr = $"SELECT t1.upline_user_id, t1.upline_subs_id, t2.setting_rank_id, sum(t1.rebate), t3.rewards_same_lvl, t2.hierarchyList, t2.upline_id FROM trade_rebate_summaries t1 INNER JOIN users t2 ON t1.upline_user_id = t2.id and t2.deleted_at is null " +
                            $"INNER JOIN setting_ranks t3 ON t3.position = t2.setting_rank_id and t3.deleted_at is null where t1.deleted_at is null and t1.upline_user_id = t1.user_id and t3.rewards_same_lvl > 0 AND t1.rebate > 0 and t1.status = 'Approved' " +
                            $"and DATE(execute_at) = '{TimeNow.ToString("yyyy-MM-dd")}' AND t1.upline_user_id not in (SELECT distinct claimed_frm_user FROM trade_same_level_reward where DATE(rebate_claimed_time) = '{TimeNow.ToString("yyyy-MM-dd")}' and claim_type = 'personal') " +
                            $"group by t1.upline_user_id, t1.upline_subs_id, t3.rewards_same_lvl ";
            
            Console.WriteLine($"proceed_same_level_rewards sqlstr - {sqlstr}");
            using (MySqlConnection sql_conn = new MySqlConnection(conn))
            {
                sql_conn.Open(); // Open the connection
                try
                {
                    Console.WriteLine($"");
                    MySqlCommand select_cmd = new MySqlCommand(sqlstr, sql_conn);
                    MySqlDataReader reader = select_cmd.ExecuteReader();
                    while (reader.Read())
                    {
                        //user id, setting_rank_id, total_rebate, rewards_slvl, hierarchy
                        object[] rewardsData = {
                            reader.IsDBNull(0) ? (object)null : reader.GetInt64(0),
                            reader.IsDBNull(1) ? (object)null : reader.GetInt64(1),
                            reader.IsDBNull(2) ? (object)null : reader.GetInt64(2),
                            reader.IsDBNull(3) ? (object)null : reader.GetDouble(3),
                            reader.IsDBNull(4) ? (object)null : reader.GetDouble(4),
                            reader.IsDBNull(5) ? (object)new String("") : reader.GetString(5),
                            reader.IsDBNull(6) ? (object)Convert.ToInt64(0) : reader.GetInt64(6)
                        };
                        same_level_list.Add(rewardsData);
                    }
                    reader.Close();
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Error sending message: " + ex.Message);
                }
            }

            foreach (var s_lvl in same_level_list)
            {
                Console.WriteLine($"personal same_level: {string.Join(", ", s_lvl)}");

                long dw_id = (long)s_lvl[0];
                long dw_subsid = (long)s_lvl[1];
                long ori_dw = dw_id;
                long dw_rank = (long)s_lvl[2];
                double total_lot_rebate = (double)s_lvl[3];
                double same_lvl_rewards_prct = (double)s_lvl[4];
                double final_rewards = 0;
                string hierlist = (string)s_lvl[5];
                //long upline_id = (long)s_lvl[5];

                if( hierlist != null && hierlist.Length > 0 )
                {
                    string[] hierlistSplit = hierlist.Split(new[] { '-' }, StringSplitOptions.RemoveEmptyEntries);
                    for (int x = hierlistSplit.Length - 1; x >= 0; x--)
                    {
                        if (hierlistSplit[x] != "")
                        {
                            long.TryParse(hierlistSplit[x], out long upline_id);
                            long upline_rank = 0; long upline_active = 0; string upline_role="";
                            final_rewards = 0;   
                            retrieve_userid(upline_id, ref upline_rank, ref upline_active, ref upline_role);

                            if (upline_active > 0 && upline_rank > 0 && upline_rank == dw_rank) //&& upline_rank == dw_rank 
                            {
                                final_rewards = (total_lot_rebate * same_lvl_rewards_prct) / 100;
                                Console.WriteLine($"{x} - upline_id: {upline_id} - upline_rank: {upline_rank} - upline_active: {upline_active} - total_lot_rebate: {total_lot_rebate} - same_lvl_rewards_prct: {same_lvl_rewards_prct} - final_rewards: {final_rewards}");
                                
                                if (final_rewards > 0)
                                {
                                    List<object[]> subs_data_list = new List<object[]>();
                                    retrieve_subscription_basedon_userid(upline_id, ref subs_data_list);
                                    
                                    /* if(subs_data_list.Count > 0)
                                    {
                                        double left_bal = final_rewards ;

                                        foreach (var subs_data in subs_data_list)
                                        {
                                            long upline_meta_login = (long) subs_data[5];
                                            long upline_sub_id = (long) subs_data[0];
                                            double quota = (double)subs_data[3];
                                            if(quota > 0 &&  left_bal > 0)
                                            {
                                                if(quota > 0 &&  left_bal > 0)
                                                {

                                                insert_update_based_rebate(upline_id, upline_rank, upline_sub_id, upline_meta_login, dw_id, dw_rank, dw_sub_id, subsid, meta_login, 
                                                           time_close, trade_volume, upline_rebate_perlot, net_rebate_lot, final_amount, quota, left_bal, remarks);


                                                long bonus_wallet_id = 0;   double bonus_wallet_oldbal = 0; long e_wallet_id = 0;   double e_wallet_oldbal = 0;
                                                retrieve_bonus_e_wallet_data( upline_id, ref bonus_wallet_id, ref bonus_wallet_oldbal, ref e_wallet_id, ref e_wallet_oldbal ); 
                                                double bonus_precent_decimal = (double)bonus_precent_given / 100; double rewards_precent_decimal = (double)rewards_precent_given / 100;

                                                // rebate divide 80% and 20%
                                                double rebate_major = Math.Round(left_bal * bonus_precent_decimal, 4); // Bonus Wallet
                                                double rebate_minor = Math.Round(left_bal * rewards_precent_decimal, 4); // E-wallets
                                                Console.WriteLine($"rebate: {left_bal} - bonus_precent_decimal: {bonus_precent_decimal} - rewards_precent_decimal: {rewards_precent_decimal}");

                                                double new_bonus_wallet = bonus_wallet_oldbal + rebate_major; double new_e_wallet = e_wallet_oldbal + rebate_minor;
                                                string remarks_major = $"LotSize Rebate (bonus_wallet) => ${Math.Round(left_bal,4)} * {bonus_precent_given}% = ${Math.Round(rebate_major,4)}";
                                                string remarks_minor = $"LotSize Rebate (e_wallet) => ${Math.Round(left_bal, 4) } * {rewards_precent_given}% = ${Math.Round(rebate_minor,4) }"; 
                                                insert_to_wallet_walletlog_transaction(0, upline_id, upline_sub_id, bonus_wallet_id, bonus_wallet_oldbal, new_bonus_wallet, "bonus", "LotSizeRebate", rebate_major, remarks_major );   
                                                insert_to_wallet_walletlog_transaction(1, upline_id, upline_sub_id, e_wallet_id, e_wallet_oldbal, new_e_wallet, "bonus", "LotSizeRebate", rebate_minor,  remarks_minor );     

                                                left_bal = left_bal - net_rebate_lot;

                                                }
                                            else if (quota < left_bal){
                                                insert_update_based_rebate(upline_id, upline_rank, upline_sub_id, upline_meta_login, dw_id, dw_rank, dw_sub_id, subsid, meta_login, 
                                                           time_close, trade_volume, upline_rebate_perlot, net_rebate_lot, final_amount, quota, quota, remarks);
                                                
                                                long bonus_wallet_id = 0;   double bonus_wallet_oldbal = 0; long e_wallet_id = 0;   double e_wallet_oldbal = 0;
                                                retrieve_bonus_e_wallet_data( upline_id, ref bonus_wallet_id, ref bonus_wallet_oldbal, ref e_wallet_id, ref e_wallet_oldbal ); 
                                                double bonus_precent_decimal = (double)bonus_precent_given / 100; double rewards_precent_decimal = (double)rewards_precent_given / 100;

                                                // rebate divide 80% and 20%
                                                double rebate_major = Math.Round(quota * bonus_precent_decimal, 4); // Bonus Wallet
                                                double rebate_minor = Math.Round(quota * rewards_precent_decimal, 4); // E-wallets
                                                Console.WriteLine($"rebate: {quota} - bonus_precent_decimal: {bonus_precent_decimal} - rewards_precent_decimal: {rewards_precent_decimal}");

                                                double new_bonus_wallet = bonus_wallet_oldbal + rebate_major; double new_e_wallet = e_wallet_oldbal + rebate_minor;
                                                string remarks_major = $"LotSize Rebate (bonus_wallet) => ${Math.Round(quota,4)} * {bonus_precent_given}% = ${Math.Round(rebate_major,4)}";
                                                string remarks_minor = $"LotSize Rebate (e_wallet) => ${Math.Round(quota, 4) } * {rewards_precent_given}% = ${Math.Round(rebate_minor,4) }"; 
                                                insert_to_wallet_walletlog_transaction(0, upline_id, upline_sub_id, bonus_wallet_id, bonus_wallet_oldbal, new_bonus_wallet, "bonus", "LotSizeRebate", rebate_major, remarks_major );   
                                                insert_to_wallet_walletlog_transaction(1, upline_id, upline_sub_id, e_wallet_id, e_wallet_oldbal, new_e_wallet, "bonus", "LotSizeRebate", rebate_minor,  remarks_minor );     
                                                left_bal = left_bal - quota;
                                                    //Console.WriteLine($" < left_bal : {left_bal}");
                                            }
                                            }
                                        } 
                                        
                                    Console.WriteLine($"");
                                    insert_to_trade_same_level_reward(dw_id, dw_rank, upline_id, upline_rank, total_lot_rebate, same_lvl_rewards_prct, final_rewards, TimeNow, "personal", ori_dw, "bonus", "SameLevelRewards" ); 
                                    
                                    
                                    } */
                                    break;
                                }
                            }
                        }
                    }
                }
            }

            taskStopwatch.Stop();
            Console.WriteLine("");
            Console.WriteLine($"Task proceed_same_level_rewards_personal completed in {taskStopwatch.Elapsed.TotalSeconds} seconds ({taskStopwatch.Elapsed.TotalMinutes})");
        }

        private static void proceed_same_level_rewards_group(DateTime TimeNow)
        {
            var taskStopwatch = Stopwatch.StartNew();
            Console.WriteLine("proceed_same_level_rewards_group ... ");

            List<object[]> same_level_list = new List<object[]>();

            string sqlstr = $"SELECT t1.upline_user_id, t2.setting_rank_id, sum(t1.rebate), t3.rewards_same_lvl, t2.hierarchyList, t2.upline_id FROM trade_rebate_summaries t1 INNER JOIN users t2 ON t1.upline_user_id = t2.id and t2.deleted_at is null " +
                            $"INNER JOIN setting_ranks t3 ON t3.position = t2.setting_rank_id and t3.deleted_at is null where t1.deleted_at is null and t1.upline_user_id != t1.user_id and t3.rewards_same_lvl > 0 AND t1.rebate > 0 and t1.status = 'Approved' " +
                            $"and DATE(execute_at) = '{TimeNow.ToString("yyyy-MM-dd")}' AND t1.upline_user_id not in (SELECT distinct claimed_frm_user FROM trade_same_level_reward where DATE(rebate_claimed_time) = '{TimeNow.ToString("yyyy-MM-dd")}' and claim_type = 'network') " +
                            $"group by t1.upline_user_id, t3.rewards_same_lvl ";

            Console.WriteLine($"proceed_same_level_rewards sqlstr - {sqlstr}");

            using (MySqlConnection sql_conn = new MySqlConnection(conn))
            {
                sql_conn.Open(); // Open the connection
                try
                {
                    Console.WriteLine($"");
                    MySqlCommand select_cmd = new MySqlCommand(sqlstr, sql_conn);
                    MySqlDataReader reader = select_cmd.ExecuteReader();
                    while (reader.Read())
                    {
                        //user id, setting_rank_id, total_rebate, rewards_slvl, hierarchy
                        object[] rewardsData = {
                            reader.IsDBNull(0) ? (object)null : reader.GetInt64(0),
                            reader.IsDBNull(1) ? (object)null : reader.GetInt64(1),
                            reader.IsDBNull(2) ? (object)null : reader.GetDouble(2),
                            reader.IsDBNull(3) ? (object)null : reader.GetDouble(3),
                            reader.IsDBNull(4) ? (object)new String("") : reader.GetString(4),
                            reader.IsDBNull(5) ? (object)Convert.ToInt64(0) : reader.GetInt64(5)
                        };
                        same_level_list.Add(rewardsData);
                    }
                    reader.Close();
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Error sending message: " + ex.Message);
                }
            }

            try
            {
                foreach (var s_lvl in same_level_list)
                {
                    Console.WriteLine($"");
                    Console.WriteLine($"group same_level: {string.Join(", ", s_lvl)}");

                    long dw_id = (long)s_lvl[0];
                    long ori_dw = dw_id;
                    long dw_rank = (long)s_lvl[1];
                    double total_lot_rebate = (double)s_lvl[2];
                    double same_lvl_rewards_prct = (double)s_lvl[3];
                    double final_rewards = 0;
                    string hierlist = (string)s_lvl[4];
                    //long upline_id = (long)s_lvl[5];

                    if( hierlist != null && hierlist.Length > 0 )
                    {
                        string[] hierlistSplit = hierlist.Split(new[] { '-' }, StringSplitOptions.RemoveEmptyEntries);
                        for (int x = hierlistSplit.Length - 1; x >= 0; x--)
                        {
                            if (hierlistSplit[x] != "")
                            {
                                long.TryParse(hierlistSplit[x], out long upline_id);
                                long upline_rank = 0; long upline_active = 0; string upline_role="";
                                final_rewards = 0;   
                                retrieve_userid(upline_id, ref upline_rank, ref upline_active, ref upline_role);

                                if (upline_active > 0 && upline_rank > 0 && upline_rank == dw_rank) // && upline_rank == dw_rank && upline_rank == dw_rank
                                {
                                    final_rewards = (total_lot_rebate * same_lvl_rewards_prct) / 100;
                                    if (final_rewards > 0)
                                    {
                                        Console.WriteLine($"");
                                        insert_to_trade_same_level_reward(dw_id, dw_rank, upline_id, upline_rank, total_lot_rebate, same_lvl_rewards_prct, final_rewards, TimeNow, "network", ori_dw, "bonus", "SameLevelRewards");
                                        break;
                                    }
                                }
                            }
                        }
                    } 
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error sending message: " + ex.Message);
            }

            taskStopwatch.Stop();
            Console.WriteLine("");
            Console.WriteLine($"Task proceed_same_level_rewards_group completed in {taskStopwatch.Elapsed.TotalSeconds} seconds ({taskStopwatch.Elapsed.TotalMinutes})");
        }

        private static void insert_to_trade_same_level_reward(long dw_id, long dw_rank, long upline_id, long upline_rank, double total_lot_rebate, double same_lvl_rewards_prct,
                                                              double final_rewards, DateTime rebate_sum_dt, string type, long from_user, string category, string purpose)
        {
            long  bonus_wallet_id = 0;
            double bonus_wallet_bal = 0;

            long  e_wallet_id = 0;
            double e_wallet_bal = 0;

            using (MySqlConnection sql_conn = new MySqlConnection(conn))
            {
                sql_conn.Open(); // Open the connection

                string sqlstr = $"INSERT INTO trade_same_level_reward( downline_id, downline_rank, upline_id, upline_rank, " +
                                $"commission_amount, bonus_percentage, bonus_amount, is_claimed, claim_type, rebate_claimed_time, claimed_frm_user, created_at ) " +
                                $"VALUES ({dw_id},{dw_rank},{upline_id},{upline_rank}, ROUND({total_lot_rebate},2), ROUND({same_lvl_rewards_prct},2), ROUND({final_rewards},2), " +
                                $"'Approved', '{type}', '{rebate_sum_dt.ToString("yyyy-MM-dd")}', {from_user}, '{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")}' );";

                Console.WriteLine($"insert_cmd sqlstr: {sqlstr}");
                MySqlCommand insert_cmd = new MySqlCommand(sqlstr, sql_conn);
                insert_cmd.ExecuteScalar();

                // retrieve wallet balance
                string bonus_wallet_sqlstr = $"select id, balance from wallets where deleted_at is null and type = 'bonus_wallet' and user_id = {upline_id};";
                MySqlCommand bonus_wallet_cmd = new MySqlCommand(bonus_wallet_sqlstr, sql_conn);
                MySqlDataReader result0 = bonus_wallet_cmd.ExecuteReader();
                while (result0.Read())
                {
                    bonus_wallet_id = result0.GetInt64(0);
                    bonus_wallet_bal = result0.GetDouble(1);
                }
                result0.Close();

                string e_wallet_sqlstr = $"select id, balance from wallets where deleted_at is null and type = 'e_wallet' and user_id = {upline_id};";
                Console.WriteLine($"e_wallet_sqlstr: {e_wallet_sqlstr}");
                MySqlCommand e_wallet_cmd = new MySqlCommand(e_wallet_sqlstr, sql_conn);
                MySqlDataReader result1 = e_wallet_cmd.ExecuteReader();
                while (result1.Read())
                {
                    e_wallet_id = result1.GetInt64(0);
                    e_wallet_bal = result1.GetDouble(1);
                }
                result1.Close();

                double bonus_precent_decimal = (double)bonus_precent_given / 100;
                double rewards_precent_decimal = (double)rewards_precent_given / 100;

                // rebate divide 80% and 20%
                double rebate_major = Math.Round(final_rewards * bonus_precent_decimal, 2); // Bonus Wallet
                double rebate_minor = Math.Round(final_rewards * rewards_precent_decimal, 2); // E-wallets

                double new_bonus_wallet = bonus_wallet_bal + rebate_major;
                double new_e_wallet = e_wallet_bal + rebate_minor;

                // --------------------------------------------------------------------- 80%
                string remarks_major = $"Same Lvl Rewards(bonus_wallet) => ${Math.Round(final_rewards, 2)} * {bonus_precent_given}% = ${rebate_major}";
                string walletmajor_sqlstr = "INSERT INTO wallet_logs (user_id, wallet_id, old_balance, new_balance, wallet_type, category, purpose, amount, remark, created_at) " +
                                            $"VALUES ({upline_id}, {bonus_wallet_id}, ROUND({bonus_wallet_bal},2), ROUND({new_bonus_wallet},2), 'bonus_wallet', '{category}', '{purpose}', " +
                                            $"ROUND({rebate_major},2), '{remarks_major}', '{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")}' );";
                Console.WriteLine($"walletmajor_sqlstr: {walletmajor_sqlstr}");
                MySqlCommand b_wallet_insert_cmd = new MySqlCommand(walletmajor_sqlstr, sql_conn);
                b_wallet_insert_cmd.ExecuteScalar();

                string walletmajor_update = $"update wallets set balance = balance + ROUND({rebate_major},2), updated_at = '{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")}' " +
                                            $"where deleted_at is null and user_id = {upline_id} and type = 'bonus_wallet' and id > 0; ";
                Console.WriteLine($"walletmajor_update: {walletmajor_update}");
                MySqlCommand b_wallet_update_cmd = new MySqlCommand(walletmajor_update, sql_conn);
                b_wallet_update_cmd.ExecuteScalar();

                string b_walletlog_id_str = $"SELECT id FROM wallet_logs where category = '{category}' and wallet_type = 'bonus_wallet' and purpose = '{purpose}' and new_balance = ROUND({new_bonus_wallet},2) " +
                                         $"and user_id = {upline_id} and DATE(created_at) = DATE('{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")}') order by created_at desc limit 1;";
                Console.WriteLine($"b_walletlog_id_str: {b_walletlog_id_str}");
                MySqlCommand b_select_cmd = new MySqlCommand(b_walletlog_id_str, sql_conn);
                long b_walletlog_id = Convert.ToInt64(b_select_cmd.ExecuteScalar());

                string major_b_transaction =  $"INSERT INTO transactions (user_id, category, transaction_type, fund_type, to_wallet_id, amount, transaction_amount, new_wallet_amount, status, comment, created_at)"+
                                              $"VALUES ( {upline_id}, 'wallet', '{purpose}', 'RealFund', {bonus_wallet_id}, ROUND({rebate_major},2), ROUND({rebate_major},2), ROUND({new_bonus_wallet},2), " +
                                              $"'Success', '{b_walletlog_id}', '{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")}'); ";
                Console.WriteLine($"major_b_transaction: {major_b_transaction}");
                MySqlCommand b_insert_transaction_cmd = new MySqlCommand(major_b_transaction, sql_conn);
                b_insert_transaction_cmd.ExecuteScalar();
                            
                // --------------------------------------------------------------------- 20%

                string remarks_minor = $"Same Lvl Rewards(e_wallet) => ${Math.Round(final_rewards, 2)} * {rewards_precent_given}% = ${rebate_minor}";
                string walletminor_sqlstr = "INSERT INTO wallet_logs (user_id, wallet_id, old_balance, new_balance, wallet_type, category, purpose, amount, remark, created_at) " +
                                            $"VALUES ({upline_id}, {e_wallet_id}, ROUND({e_wallet_bal},2), ROUND({new_e_wallet},2), 'e_wallet', '{category}', '{purpose}', "+
                                            $"ROUND({rebate_minor},2), '{remarks_minor}', '{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")}' );";
                Console.WriteLine($"walletminor_sqlstr: {walletminor_sqlstr}");
                MySqlCommand e_wallet_insert_cmd = new MySqlCommand(walletminor_sqlstr, sql_conn);
                e_wallet_insert_cmd.ExecuteScalar();

                string walletminor_update = $"update wallets set balance = balance + ROUND({rebate_minor},2), updated_at = '{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")}' " +
                                            $"where deleted_at is null and user_id = {upline_id} and type = 'e_wallet' and id > 0; ";
                Console.WriteLine($"walletminor_update: {walletminor_update}");
                MySqlCommand e_wallet_update_cmd = new MySqlCommand(walletminor_update, sql_conn);
                e_wallet_update_cmd.ExecuteScalar();

                string e_walletlog_id_str = $"SELECT id FROM wallet_logs where category = '{category}' and wallet_type = 'e_wallet' and purpose = '{purpose}' and new_balance = ROUND({new_e_wallet},2) " +
                                         $"and user_id = {upline_id} and DATE(created_at) = DATE('{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")}') order by created_at desc limit 1;";
                Console.WriteLine($"e_walletlog_id_str: {e_walletlog_id_str}");
                MySqlCommand e_select_cmd = new MySqlCommand(e_walletlog_id_str, sql_conn);
                long e_walletlog_id = Convert.ToInt64(e_select_cmd.ExecuteScalar());

                string minor_e_transaction =  $"INSERT INTO transactions (user_id, category, transaction_type, fund_type, to_wallet_id, amount, transaction_amount, new_wallet_amount, status, comment, created_at)"+
                                              $"VALUES ({upline_id}, 'wallet', '{purpose}', 'RealFund', {e_wallet_id},  ROUND({rebate_minor},2), ROUND({rebate_minor},2), ROUND({new_e_wallet},2), " +
                                              $"'Success', '{e_walletlog_id}', '{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")}'); ";
                Console.WriteLine($"minor_e_transaction: {minor_e_transaction}");
                MySqlCommand e_insert_transaction_cmd = new MySqlCommand(minor_e_transaction, sql_conn);
                e_insert_transaction_cmd.ExecuteScalar();
            }
        }

        private static async Task Telegram_Send(string messages)
        {
            //Console.WriteLine("Enter Telegram_Send - "+messages);
            string telegramApiToken_0 = telegramApiToken;
            long chatId_0 = chatId;
            var botClient = new TelegramBotClient(telegramApiToken_0);
            //var me = await botClient.GetMeAsync();
            //Console.WriteLine($"Hello, World! I am user {me.Id} and my name is {me.FirstName}.");
            //Console.WriteLine(" Telegram_Send "+botClient );
            Console.WriteLine(" Telegram_Send " + (title_name + messages));

            try
            {
                await botClient.SendTextMessageAsync(chatId_0, (title_name + messages));
                Console.WriteLine("Message sent successfully.");
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error sending message: " + ex.Message);
            }
        }

        private static void insert_ranking_logs_update_user(object[] user_info_list)
        {
            using (MySqlConnection sql_conn = new MySqlConnection(conn))
            {
                sql_conn.Open(); // Open the connection
                string sqlstr = "";
                //foreach (var user_info_list in user_rank_adjust_list)
                //{
                //0-user_id, 1-user_rank, 2-personal_require, 3-target_personal_require, 4-direct_referral, 5-target_direct_referral, 
                //6-group_sales, 7-target_group_sales, 8-least_rank 
                if (user_info_list.Length == 9)
                {
                    sqlstr = "INSERT INTO ranking_logs (user_id, old_rank, new_rank, user_package_amount, target_package_amount, user_direct_referral_amount, target_direct_referral_amount, " +
                             "user_group_sales, target_group_sales, created_at) VALUES (" +
                             $"{user_info_list[0]},{user_info_list[1]},{user_info_list[8]}, ROUND({user_info_list[2]},2), ROUND({user_info_list[3]},2), {user_info_list[4]}, {user_info_list[5]}, " +
                             $"{user_info_list[6]},{user_info_list[7]},'{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")}');";
                    Console.WriteLine($"insert_ranking_logs_update_user layer 1: {sqlstr}");
                    MySqlCommand insert_cmd = new MySqlCommand(sqlstr, sql_conn);
                    insert_cmd.ExecuteScalar();

                    sqlstr = $"UPDATE users SET setting_rank_id = {user_info_list[8]}, updated_at = '{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")}' WHERE id = {user_info_list[0]}; ";
                    MySqlCommand update_cmd = new MySqlCommand(sqlstr, sql_conn);
                    update_cmd.ExecuteScalar();
                }

                //0-user_id, 1-user_rank, 2-personal_require, 3-target_personal_require, 4-package_req_rank,
                //5-direct_referral, 6-target_direct_referral, 7-dir_referral_rank,
                //8-group_sales, 9-target_group_sales, 10-group_sales_rank,
                //11-least_rank
                // second layer - out list
                if (user_info_list.Length == 12)
                {
                    sqlstr = "INSERT INTO ranking_logs (user_id, old_rank, new_rank, user_package_amount, target_package_amount, user_direct_referral_amount, target_direct_referral_amount, " +
                             "user_group_sales, target_group_sales, created_at) VALUES (" +
                             $"{user_info_list[0]},{user_info_list[1]},{user_info_list[11]}, ROUND({user_info_list[2]},2), ROUND({user_info_list[3]},2), {user_info_list[5]}, {user_info_list[6]}, " +
                             $"{user_info_list[8]},{user_info_list[9]},'{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")}');";
                    Console.WriteLine($"insert_ranking_logs_update_user layer 2: {sqlstr}");
                    MySqlCommand insert_cmd = new MySqlCommand(sqlstr, sql_conn);
                    insert_cmd.ExecuteScalar();

                    sqlstr = $"UPDATE users SET setting_rank_id = {user_info_list[11]}, updated_at = '{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")}' WHERE id = {user_info_list[0]}; ";
                    MySqlCommand update_cmd = new MySqlCommand(sqlstr, sql_conn);
                    update_cmd.ExecuteScalar();
                }

                if (user_info_list.Length == 16)
                {
                    sqlstr = "INSERT INTO ranking_logs (user_id, old_rank, new_rank, user_package_amount, target_package_amount, user_direct_referral_amount, target_direct_referral_amount, " +
                             "user_group_sales, target_group_sales, user_cultivate_member_amount, target_cultivate_member_amount, target_cultivate_type_id, referrals_with_target_cultivate_type, created_at) VALUES (" +
                             $"{user_info_list[0]},{user_info_list[1]},{user_info_list[11]}, ROUND({user_info_list[2]},2), ROUND({user_info_list[3]},2), {user_info_list[5]}, {user_info_list[6]}, " +
                             $"{user_info_list[8]},{user_info_list[9]}, {user_info_list[12]}, {user_info_list[13]}, {user_info_list[14]}, '{user_info_list[15]}', '{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")}');";
                    Console.WriteLine($"insert_ranking_logs_update_user layer 2: {sqlstr}");
                    MySqlCommand insert_cmd = new MySqlCommand(sqlstr, sql_conn);
                    insert_cmd.ExecuteScalar();

                    sqlstr = $"UPDATE users SET setting_rank_id = {user_info_list[11]}, updated_at = '{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")}' WHERE id = {user_info_list[0]}; ";
                    MySqlCommand update_cmd = new MySqlCommand(sqlstr, sql_conn);
                    update_cmd.ExecuteScalar();
                }
            }
        }

        private static void ranking_second_layer(List<object[]> user_list, string waiting_list, string exclude_list)
        {
            long min_rank_WOut_cultivate = 1;
            List<object[]> cultivate_list = new List<object[]>();

            using (MySqlConnection sql_conn = new MySqlConnection(conn))
            {
                sql_conn.Open(); // Open the connection
                string min_r_sqlstr = "SELECT max(position) FROM setting_ranks where deleted_at is null and cultivate_type is null and cultivate_member is null " +
                                        "and cultivate_ranktype is null ; ";
                MySqlCommand min_r_cmd = new MySqlCommand(min_r_sqlstr, sql_conn);
                object result = min_r_cmd.ExecuteScalar();
                if (result != null)
                {
                    min_rank_WOut_cultivate = Convert.ToInt64(result);
                }

                //object[] cultivate_obj = new object[0];
                string cultivate_sqlstr = "SELECT position, cultivate_ranktype, cultivate_member FROM setting_ranks where deleted_at is null and cultivate_type is not null and cultivate_member is not null " +
                                        "and cultivate_ranktype is not null order by position asc; ";

                MySqlCommand select_cmd = new MySqlCommand(cultivate_sqlstr, sql_conn);
                MySqlDataReader reader = select_cmd.ExecuteReader();
                while (reader.Read())
                {
                    object[] cultiData = { reader.GetInt64(0), reader.GetInt64(1), reader.GetInt64(2) };
                    cultivate_list.Add(cultiData);
                }
                reader.Close();
            }
            Console.WriteLine($"cultivate_list: {cultivate_list.Count}");
            //Console.WriteLine($"cultivate_obj: {cultivate_obj.Length}");

            // condition1 - 1- personal_req, 2-group_sales, direct_referral
            // user_id , current_rank, status(up, down), condition1 {1,2,3,4}, cultivate_type, cultivate_amt, who_they, fulfill_rank
            List<object[]> queue_List = new List<object[]>();
            using (MySqlConnection sql_conn = new MySqlConnection(conn))
            {
                sql_conn.Open(); // Open the connection
                foreach (var user_no in user_list)
                {
                    string user_sqlstr = $"SELECT count(id) from users where hierarchyList LIKE CONCAT('%-', {user_no[0]}, '-%') and id IN ({waiting_list}); ";
                    MySqlCommand select_cmd = new MySqlCommand(user_sqlstr, sql_conn);
                    MySqlDataReader reader = select_cmd.ExecuteReader();
                    while (reader.Read())
                    {
                        //user id, setting_rank_id
                        object[] userData = { user_no[0], reader.GetInt64(0) };
                        queue_List.Add(userData);
                    }
                    reader.Close();
                }
            }

            if (queue_List.Count > 0)
            {
                var queue_List1 = queue_List.OrderBy(item => (long)item[1]).ToList();
                foreach (var user in queue_List1)
                {
                    long user_id = (long)user[0];
                    List<object[]> direct_referral_List = new List<object[]>();
                    using (MySqlConnection sql_conn = new MySqlConnection(conn))
                    {
                        sql_conn.Open(); // Open the connection
                        Console.WriteLine("");
                        Console.WriteLine($"user id: {user_id}, referrals effect: {user[1]}");
                        string user_sqlstr = $"SELECT id, setting_rank_id FROM users WHERE deleted_at is null and role='user' and status = 'Active' and upline_id = {user_id} ; ";
                        MySqlCommand select_cmd = new MySqlCommand(user_sqlstr, sql_conn);
                        MySqlDataReader reader = select_cmd.ExecuteReader();
                        while (reader.Read())
                        {
                            //0-user id, 1-setting_rank_id, 2-max_downlines_rank, 3-final_achieve, who they are
                            object[] dir_referralData = { reader.GetInt64(0), reader.GetInt64(1), 1, 1, "" };
                            direct_referral_List.Add(dir_referralData);
                        }
                        reader.Close();
                    }

                    if (direct_referral_List.Count > 0)
                    {
                        //Console.WriteLine($"direct_referral_List: {direct_referral_List.Count}");
                        string lines_max_downlines = "";
                        // check every lines
                        foreach (var dir_ref_info in direct_referral_List)
                        {
                            long direct_referral_id = (long)dir_ref_info[0];
                            long direct_referral_rank = (long)dir_ref_info[1];
                            long direct_referral_dwrank = 1;
                            using (MySqlConnection sql_conn = new MySqlConnection(conn))
                            {
                                sql_conn.Open(); // Open the connection
                                string dir_ref_sqlstr = $"SELECT COALESCE(max(setting_rank_id),1) FROM users WHERE deleted_at is null and role='user' and status = 'Active' and hierarchyList LIKE '%-{direct_referral_id}-%'; ";
                                //Console.WriteLine($"dir_ref_sqlstr: {dir_ref_sqlstr}");
                                MySqlCommand select_cmd = new MySqlCommand(dir_ref_sqlstr, sql_conn);
                                object result = select_cmd.ExecuteScalar();
                                if (result != null)
                                {
                                    dir_ref_info[2] = Convert.ToInt64(result);
                                    direct_referral_dwrank = (long)Convert.ToInt64(result);
                                }

                                dir_ref_info[3] = Math.Max(direct_referral_rank, direct_referral_dwrank);

                                // referrals which fulfill stored in string 
                                if ((long)dir_ref_info[3] == direct_referral_rank)
                                {
                                    lines_max_downlines = lines_max_downlines + $"|{direct_referral_id}:R{direct_referral_rank}";

                                }
                                else if ((long)dir_ref_info[3] == direct_referral_dwrank)
                                {
                                    dir_ref_sqlstr = $"SELECT id FROM users WHERE deleted_at is null and role='user' and status = 'Active' and hierarchyList LIKE '%-{direct_referral_id}-%' " +
                                                     $"and setting_rank_id = {direct_referral_dwrank} ORDER BY LENGTH(hierarchyList) asc limit 1; ";
                                    MySqlCommand select_cmd1 = new MySqlCommand(dir_ref_sqlstr, sql_conn);
                                    object result1 = select_cmd1.ExecuteScalar();
                                    if (result1 != null)
                                    {
                                        lines_max_downlines = lines_max_downlines + $"|{Convert.ToInt64(result1)}:R{direct_referral_dwrank}";
                                    }
                                }
                            }
                        }

                        // sum up every lines and check rank and amount
                        var referralsCounts = direct_referral_List.GroupBy(user => (long)user[3]) // Group by the values in the second column
                            .ToDictionary(group => group.Key, group => group.Count()); // Create a dictionary with counts

                        long rank_confirmed = min_rank_WOut_cultivate;
                        object[] fulfill_data = new object[4] { 0, 0, 0, "" };
                        long member_amt_confirmed = 0;
                        long target_member_confirmed = 0;

                        //Key - Referral Rank, Value - Count of Them
                        foreach (var ref_Count in referralsCounts.OrderByDescending(x => x.Key))
                        {
                            var cultivate_fulfill = cultivate_list.FirstOrDefault(rank => (long)rank[1] == (long)ref_Count.Key);
                            if (cultivate_fulfill != null)
                            {
                                long cultivate_rank = (long)cultivate_fulfill[0];
                                long cultivate_type = (long)cultivate_fulfill[1];
                                long cultivate_member = (long)cultivate_fulfill[2];

                                if ((long)ref_Count.Key == cultivate_type)
                                {
                                    //Console.WriteLine($"ref_Count.Value: {ref_Count.Value} >= cultivate_member: {cultivate_member}");
                                    if (ref_Count.Value >= cultivate_member)
                                    {
                                        rank_confirmed = cultivate_rank;
                                        member_amt_confirmed = (long)ref_Count.Value;
                                        target_member_confirmed = (long)ref_Count.Value;
                                        fulfill_data[0] = member_amt_confirmed;
                                        fulfill_data[1] = target_member_confirmed;
                                        fulfill_data[2] = cultivate_type;
                                        fulfill_data[3] = lines_max_downlines;
                                        break;
                                    }
                                }
                            }
                        }

                        object[] df_obj = user_list.FirstOrDefault(user => (long)user[0] == user_id);
                        if (df_obj != null)
                        {
                            //Console.WriteLine($" ****************  obj length {df_obj.Length}");
                            long user_rank = (long)df_obj[1];
                            rank_confirmed = Math.Min((long)df_obj[11], rank_confirmed);

                            if (rank_confirmed > user_rank)
                            {
                                df_obj[11] = rank_confirmed;
                                df_obj = df_obj.Concat(fulfill_data).ToArray();
                                insert_ranking_logs_update_user(df_obj);
                            }
                        }
                    }
                    else
                    {
                        var no_df_obj = user_list.FirstOrDefault(user => (long)user[0] == user_id);
                        if (no_df_obj != null)
                        {
                            long user_rank = (long)no_df_obj[1];
                            if (user_rank < min_rank_WOut_cultivate)
                            {
                                no_df_obj[11] = min_rank_WOut_cultivate;
                                insert_ranking_logs_update_user(no_df_obj);
                            }
                        }
                    }
                }
            }
        }

        private static void proceed_ranking()
        {
            var taskStopwatch = Stopwatch.StartNew();
            Console.WriteLine("proceed_ranking ... ");
            try
            {
                string exclude_list = "0";
                string proceed_list = "0";
                List<object[]> userlist_forcheck = ranking_first_layer(ref exclude_list, ref proceed_list);
                Console.WriteLine("");
                // cultivate check
                if (proceed_list.Length > 1)
                {
                    ranking_second_layer(userlist_forcheck, proceed_list, exclude_list);
                }

            }
            catch (Exception ex)
            {
                Console.WriteLine($"An exception occurred: {ex}");
            }
            taskStopwatch.Stop();
            Console.WriteLine("");
            Console.WriteLine($"Task proceed_ranking completed in {taskStopwatch.Elapsed.TotalSeconds} seconds ({taskStopwatch.Elapsed.TotalMinutes})");
        }

        private static List<object[]> ranking_first_layer(ref string exclude_userlist, ref string fulfill_userlist)
        {
            // condition check 1- personal_req, 2-group_sales, direct_referral
            List<object[]> proceed_userList = new List<object[]>();
            List<object[]> downgrade_userList = new List<object[]>();

            List<object[]> userId_List = new List<object[]>();
            string user_sqlstr = $"SELECT id, setting_rank_id FROM users WHERE deleted_at is null and role='user' and status = 'Active' AND rank_up_status = 'auto' " +
                                 $"and (id = {lucky_ant_id} or top_leader_id = {lucky_ant_id}) ;";
            
            Console.WriteLine($"user_sqlstr: {user_sqlstr}");
            using (MySqlConnection sql_conn = new MySqlConnection(conn))
            {
                sql_conn.Open(); // Open the connection
                MySqlCommand select_cmd = new MySqlCommand(user_sqlstr, sql_conn);
                MySqlDataReader reader = select_cmd.ExecuteReader();
                while (reader.Read())
                {
                    //user id, setting_rank_id
                    object[] userData = { reader.GetInt64(0), reader.GetInt64(1) };
                    userId_List.Add(userData);
                }
                reader.Close();
            }

            if (userId_List.Count > 0)
            {
                using (MySqlConnection sql_conn = new MySqlConnection(conn))
                {
                    sql_conn.Open(); // Open the connection
                    foreach (var user_info in userId_List)
                    {
                        long user_id = (long)user_info[0];
                        long user_rank = (long)user_info[1];
                        if (user_id > 0)
                        {
                            double personal_require = 0;
                            long direct_referral = 0;
                            double group_sales = 0;

                            double target_personal_require = 0;
                            long target_direct_referral = 0;
                            double target_group_sales = 0;

                            long package_req_rank = 1;
                            long dir_referral_rank = 1;
                            long group_sales_rank = 1;
                            long least_rank = 1;

                            // package requirement
                            string sqlstr = $"select coalesce(sum(meta_balance),0) from subscriptions where deleted_at is null and meta_balance is not null and status = 'Active' and user_id = {user_id};";
                            MySqlCommand select_cmd = new MySqlCommand(sqlstr, sql_conn);
                            object result = select_cmd.ExecuteScalar();
                            if (result != null)
                            {
                                personal_require = Convert.ToDouble(result);
                            }

                            // direct referral requirement
                            sqlstr = "SELECT COUNT(*) AS no FROM ( SELECT user_id , sum(meta_balance) as dw_balance from subscriptions where deleted_at is null and meta_balance is not null and meta_balance > 0 " +
                                    $" and status = 'Active' and user_id in (select id from users where deleted_at is null and status = 'Active' and upline_id ={user_id})  group by user_id ) AS subquery WHERE dw_balance > 0";
                            select_cmd = new MySqlCommand(sqlstr, sql_conn);
                            object result1 = select_cmd.ExecuteScalar();
                            if (result1 != null)
                            {
                                direct_referral = Convert.ToInt64(result1);
                            }

                            // group sales
                            sqlstr = "SELECT COALESCE(sum(dw_balance),0) AS no FROM ( SELECT user_id , sum(meta_balance) as dw_balance from subscriptions where deleted_at is null and status = 'Active' " +
                                    "and meta_balance is not null and meta_balance > 0 and user_id in (select id from users where deleted_at is null and status = 'Active' " +
                                    $"and hierarchyList LIKE '%-{user_id}-%' ) group by user_id) AS subquery WHERE dw_balance > 0";
                            select_cmd = new MySqlCommand(sqlstr, sql_conn);
                            object result2 = select_cmd.ExecuteScalar();
                            if (result2 != null)
                            {
                                group_sales = Convert.ToDouble(result2);
                            }

                            // check package requirement
                            sqlstr = $"SELECT MAX(COALESCE(position,1)), max(package_requirement) FROM setting_ranks WHERE deleted_at is null and package_requirement <= {personal_require};";
                            select_cmd = new MySqlCommand(sqlstr, sql_conn);
                            MySqlDataReader reader = select_cmd.ExecuteReader();
                            while (reader.Read())
                            {
                                package_req_rank = (long)reader.GetInt64(0);
                                target_personal_require = (double)reader.GetDouble(1);
                            }
                            reader.Close();

                            // check direct referral requirement
                            sqlstr = $"SELECT MAX(COALESCE(position,1)), max(direct_referral) FROM setting_ranks WHERE deleted_at is null and direct_referral <= {direct_referral};";
                            select_cmd = new MySqlCommand(sqlstr, sql_conn);
                            reader = select_cmd.ExecuteReader();
                            while (reader.Read())
                            {
                                dir_referral_rank = (long)reader.GetInt64(0);
                                target_direct_referral = (long)reader.GetInt64(1);
                            }
                            reader.Close();

                            // check group sales
                            sqlstr = $"SELECT MAX(COALESCE(position,1)), max(group_sales) FROM setting_ranks WHERE deleted_at is null and group_sales <= {group_sales};";
                            select_cmd = new MySqlCommand(sqlstr, sql_conn);
                            reader = select_cmd.ExecuteReader();
                            while (reader.Read())
                            {
                                group_sales_rank = (long)reader.GetInt64(0);
                                target_group_sales = (double)reader.GetDouble(1);
                            }
                            reader.Close();

                            least_rank = Math.Min(package_req_rank, Math.Min(dir_referral_rank, group_sales_rank));

                            //upgrade
                            if (user_rank < least_rank)
                            {
                                // user_id , current_rank, package_req_rank, dir_referral_rank, group_sales_rank,least_rank
                                object[] userData = { user_id, user_rank,
                                                    personal_require, target_personal_require, package_req_rank,
                                                    direct_referral, target_direct_referral, dir_referral_rank,
                                                    group_sales, target_group_sales, group_sales_rank,
                                                    least_rank };

                                proceed_userList.Add(userData);
                                fulfill_userlist = $"{fulfill_userlist},{user_id}";
                            } //downgrade or no adjust
                            else if (user_rank >= least_rank)
                            {
                                exclude_userlist = $"{exclude_userlist},{user_id}";
                                //downgrade
                                if (user_rank > least_rank)
                                {
                                    object[] dwgrade_userData = { user_id, user_rank,personal_require, target_personal_require,
                                                                direct_referral, target_direct_referral, group_sales, target_group_sales, least_rank };

                                    downgrade_userList.Add(dwgrade_userData); // 9 columns
                                }
                            }

                            Console.WriteLine($"U:{user_id}, R:{user_rank}, P->{personal_require}, {target_personal_require}, {package_req_rank}, DF->{direct_referral}, {target_direct_referral}, {dir_referral_rank}, G->{group_sales}, {target_group_sales}, {group_sales_rank}, NowR:{least_rank}");
                        }
                    }
                }
            }
            Console.WriteLine($"fulfill_userlist: {fulfill_userlist}");
            Console.WriteLine($"exclude_userlist: {exclude_userlist}");
            if (downgrade_userList.Count > 0)
            {
                foreach (var dwgrade_user in downgrade_userList)
                {
                    insert_ranking_logs_update_user(dwgrade_user);
                }
            }
            // passed who next further check
            return proceed_userList;
        }

        private static void proceed_manual_ranking()
        {
            var taskStopwatch = Stopwatch.StartNew();
            Console.WriteLine("proceed_manual_ranking ... ");
            try
            {
                string exclude_list = "0";
                string proceed_list = "0";
                List<object[]> userlist_forcheck = ranking_manual_first_layer(ref exclude_list, ref proceed_list);
                //Console.WriteLine("");
                // cultivate check
                if (proceed_list.Length > 1)
                {
                    ranking_manual_second_layer(userlist_forcheck, proceed_list, exclude_list);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"An exception occurred: {ex}");
            }
            taskStopwatch.Stop();
            Console.WriteLine("");
            Console.WriteLine($"Task proceed_manual_ranking completed in {taskStopwatch.Elapsed.TotalSeconds} seconds ({taskStopwatch.Elapsed.TotalMinutes})");
        }

        private static List<object[]> ranking_manual_first_layer(ref string exclude_userlist, ref string fulfill_userlist)
        {
            // condition check 1- personal_req, 2-group_sales, direct_referral
            List<object[]> proceed_userList = new List<object[]>();
            List<object[]> downgrade_userList = new List<object[]>();

            List<object[]> userId_List = new List<object[]>();
            string user_sqlstr = $"SELECT id,setting_rank_id FROM users WHERE deleted_at is null and role='user' and status = 'Active' AND rank_up_status = 'manual' " +
                                 $"and (id = {lucky_ant_id} or top_leader_id = {lucky_ant_id}) ;";
            //$"and id not in (select id from ranking_logs where deleted_at is null and DATE(created_at) = DATE({currentD.ToString("yyyy-MM-dd")}))//; ";
           
            Console.WriteLine("user_sqlstr: "+user_sqlstr);
            using (MySqlConnection sql_conn = new MySqlConnection(conn))
            {
                sql_conn.Open(); // Open the connection
                MySqlCommand select_cmd = new MySqlCommand(user_sqlstr, sql_conn);
                MySqlDataReader reader = select_cmd.ExecuteReader();
                while (reader.Read())
                {
                    //user id, setting_rank_id
                    object[] userData = { reader.GetInt64(0), reader.GetInt64(1) };
                    userId_List.Add(userData);
                }
                reader.Close();
            }

            if (userId_List.Count > 0)
            {
                using (MySqlConnection sql_conn = new MySqlConnection(conn))
                {
                    sql_conn.Open(); // Open the connection
                    foreach (var user_info in userId_List)
                    {
                        long user_id = (long)user_info[0];
                        long user_rank = (long)user_info[1];
                        if (user_id > 0)
                        {
                            double personal_require = 0;
                            long direct_referral = 0;
                            double group_sales = 0;

                            double target_personal_require = 0;
                            long target_direct_referral = 0;
                            double target_group_sales = 0;

                            long package_req_rank = 1;
                            long dir_referral_rank = 1;
                            long group_sales_rank = 1;
                            long least_rank = 1;

                            // package requirement
                            string sqlstr = $"select coalesce(sum(meta_balance),0) from subscriptions where deleted_at is null and meta_balance is not null and status = 'Active' and user_id = {user_id};";
                            //Console.WriteLine(" package requirement sqlstr: "+sqlstr);
                            MySqlCommand select_cmd = new MySqlCommand(sqlstr, sql_conn);
                            object result = select_cmd.ExecuteScalar();
                            if (result != null)
                            {
                                personal_require = Convert.ToDouble(result);
                            }

                            // direct referral requirement
                            sqlstr = "SELECT COUNT(*) AS no FROM ( SELECT user_id , sum(meta_balance) as dw_balance from subscriptions where deleted_at is null and meta_balance is not null and meta_balance > 0 " +
                                    $" and status = 'Active' and user_id in (select id from users where deleted_at is null and status = 'Active' and upline_id ={user_id})  group by user_id ) AS subquery WHERE dw_balance > 0";
                            //Console.WriteLine("direct referral requirement sqlstr: "+sqlstr);
                            select_cmd = new MySqlCommand(sqlstr, sql_conn);
                            object result1 = select_cmd.ExecuteScalar();
                            if (result1 != null)
                            {
                                direct_referral = Convert.ToInt64(result1);
                            }

                            // group sales
                            sqlstr = "SELECT COALESCE(sum(dw_balance),0) AS no FROM ( SELECT user_id , sum(meta_balance) as dw_balance from subscriptions where deleted_at is null and status = 'Active' " +
                                    "and meta_balance is not null and meta_balance > 0 and user_id in (select id from users where deleted_at is null and status = 'Active' " +
                                    $"and hierarchyList LIKE '%-{user_id}-%' ) group by user_id) AS subquery WHERE dw_balance > 0";
                            //Console.WriteLine("group sales sqlstr: "+sqlstr);
                            select_cmd = new MySqlCommand(sqlstr, sql_conn);
                            object result2 = select_cmd.ExecuteScalar();
                            if (result2 != null)
                            {
                                group_sales = Convert.ToDouble(result2);
                            }

                            // check package requirement
                            sqlstr = $"SELECT MAX(COALESCE(position,1)), max(package_requirement) FROM setting_ranks WHERE deleted_at is null and package_requirement <= {personal_require};";
                            //Console.WriteLine("check package requirement sqlstr: "+sqlstr);
                            select_cmd = new MySqlCommand(sqlstr, sql_conn);
                            MySqlDataReader reader = select_cmd.ExecuteReader();
                            while (reader.Read())
                            {
                                package_req_rank = (long)reader.GetInt64(0);
                                target_personal_require = (double)reader.GetDouble(1);
                            }
                            reader.Close();

                            // check direct referral requirement
                            sqlstr = $"SELECT MAX(COALESCE(position,1)), max(direct_referral) FROM setting_ranks WHERE deleted_at is null and direct_referral <= {direct_referral};";
                            //Console.WriteLine("check direct referral requirement sqlstr: "+sqlstr);
                            select_cmd = new MySqlCommand(sqlstr, sql_conn);
                            reader = select_cmd.ExecuteReader();
                            while (reader.Read())
                            {
                                dir_referral_rank = (long)reader.GetInt64(0);
                                target_direct_referral = (long)reader.GetInt64(1);
                            }
                            reader.Close();

                            // check group sales
                            sqlstr = $"SELECT MAX(COALESCE(position,1)), max(group_sales) FROM setting_ranks WHERE deleted_at is null and group_sales <= {group_sales};";
                            //Console.WriteLine("check group sales sqlstr: "+sqlstr);
                            select_cmd = new MySqlCommand(sqlstr, sql_conn);
                            reader = select_cmd.ExecuteReader();
                            while (reader.Read())
                            {
                                group_sales_rank = (long)reader.GetInt64(0);
                                target_group_sales = (double)reader.GetDouble(1);
                            }
                            reader.Close();

                            //Console.WriteLine($"package_req_rank: {package_req_rank}, dir_referral_rank: {dir_referral_rank}, group_sales_rank: {group_sales_rank}");
                            least_rank = Math.Min(package_req_rank, Math.Min(dir_referral_rank, group_sales_rank));
                            //upgrade
                            if (user_rank < least_rank)
                            {
                                // user_id , current_rank, package_req_rank, dir_referral_rank, group_sales_rank,least_rank
                                object[] userData = { user_id, user_rank,
                                                    personal_require, target_personal_require, package_req_rank,
                                                    direct_referral, target_direct_referral, dir_referral_rank,
                                                    group_sales, target_group_sales, group_sales_rank,
                                                    least_rank };

                                proceed_userList.Add(userData);
                                fulfill_userlist = $"{fulfill_userlist},{user_id}";
                                
                                Console.WriteLine($"U:{user_id}, R:{user_rank}, P->{personal_require}, {target_personal_require}, {package_req_rank}, DF->{direct_referral}, {target_direct_referral}, {dir_referral_rank}, G->{group_sales}, {target_group_sales}, {group_sales_rank}, NowR:{least_rank}");
                                //Console.WriteLine($"U:{user_id}, R:{user_rank}, DF->{direct_referral}, {target_direct_referral}, {dir_referral_rank}, NowR:{least_rank}");
                            } 
                            //Console.WriteLine($"U:{user_id}, R:{user_rank}, P->{personal_require}, {target_personal_require}, {package_req_rank}, DF->{direct_referral}, {target_direct_referral}, {dir_referral_rank}, G->{group_sales}, {target_group_sales}, {group_sales_rank}, NowR:{least_rank}");
                        }
                    }
                }
            }
            Console.WriteLine($"fulfill_userlist: {fulfill_userlist} | exclude_userlist: {exclude_userlist}");

            // passed who next further check
            return proceed_userList;
        }

        private static void ranking_manual_second_layer(List<object[]> user_list, string waiting_list, string exclude_list)
        {
            //Console.WriteLine($" ----- ranking_manual_second_layer ---- ");
            long min_rank_WOut_cultivate = 1;
            List<object[]> cultivate_list = new List<object[]>();

            using (MySqlConnection sql_conn = new MySqlConnection(conn))
            {
                sql_conn.Open(); // Open the connection
                string min_r_sqlstr = "SELECT max(position) FROM setting_ranks where deleted_at is null and cultivate_type is null and cultivate_member is null and cultivate_ranktype is null ; ";
                MySqlCommand min_r_cmd = new MySqlCommand(min_r_sqlstr, sql_conn);
                object result = min_r_cmd.ExecuteScalar();
                if (result != null)
                {
                    min_rank_WOut_cultivate = Convert.ToInt64(result);
                }

                //object[] cultivate_obj = new object[0];
                string cultivate_sqlstr = "SELECT position, cultivate_ranktype, cultivate_member FROM setting_ranks where deleted_at is null and cultivate_type is not null and cultivate_member is not null " +
                                        "and cultivate_ranktype is not null order by position asc; ";
    
                MySqlCommand select_cmd = new MySqlCommand(cultivate_sqlstr, sql_conn);
                MySqlDataReader reader = select_cmd.ExecuteReader();
                while (reader.Read())
                {
                    object[] cultiData = { reader.GetInt64(0), reader.GetInt64(1), reader.GetInt64(2) };
                    cultivate_list.Add(cultiData);
                }
                reader.Close();
            }
            //Console.WriteLine($"cultivate_list: {cultivate_list.Count}");  //Console.WriteLine($"cultivate_obj: {cultivate_obj.Length}");
            // condition1 - 1- personal_req, 2-group_sales, direct_referral
            // user_id , current_rank, status(up, down), condition1 {1,2,3,4}, cultivate_type, cultivate_amt, who_they, fulfill_rank
            List<object[]> queue_List = new List<object[]>();
            using (MySqlConnection sql_conn = new MySqlConnection(conn))
            {
                sql_conn.Open(); // Open the connection
                foreach (var user_no in user_list)
                {
                    string user_sqlstr = $"SELECT count(id) from users where hierarchyList LIKE CONCAT('%-', {user_no[0]}, '-%') and id IN ({waiting_list}); ";
                    MySqlCommand select_cmd = new MySqlCommand(user_sqlstr, sql_conn);
                    MySqlDataReader reader = select_cmd.ExecuteReader();
                    while (reader.Read())
                    {
                        //user id, setting_rank_id
                        object[] userData = { user_no[0], reader.GetInt64(0) };
                        queue_List.Add(userData);
                    }
                    reader.Close();
                }
            }

            if (queue_List.Count > 0)
            {
                var queue_List1 = queue_List.OrderBy(item => (long)item[1]).ToList();
                foreach (var user in queue_List1)
                {
                    long user_id = (long)user[0];
                    List<object[]> direct_referral_List = new List<object[]>();
                    using (MySqlConnection sql_conn = new MySqlConnection(conn))
                    {
                        sql_conn.Open(); // Open the connection
                        Console.WriteLine("");
                        Console.WriteLine($"user id: {user_id}, referrals effect: {user[1]}");
                        string user_sqlstr = $"SELECT id, setting_rank_id FROM users WHERE deleted_at is null and role='user' and status = 'Active' and upline_id = {user_id} ; ";
                        MySqlCommand select_cmd = new MySqlCommand(user_sqlstr, sql_conn);
                        MySqlDataReader reader = select_cmd.ExecuteReader();
                        while (reader.Read())
                        {
                            //0-user id, 1-setting_rank_id, 2-max_downlines_rank, 3-final_achieve, who they are
                            object[] dir_referralData = { reader.GetInt64(0), reader.GetInt64(1), 1, 1, "" };
                            direct_referral_List.Add(dir_referralData);
                        }
                        reader.Close();
                    }

                    //Console.WriteLine(" ----------------------------------------------------- ");
                    if (direct_referral_List.Count > 0)
                    {
                        //Console.WriteLine($"direct_referral_List: {direct_referral_List.Count}");
                        string lines_max_downlines = "";
                        // check every lines
                        foreach (var dir_ref_info in direct_referral_List)
                        {
                            long direct_referral_id = (long)dir_ref_info[0];
                            long direct_referral_rank = (long)dir_ref_info[1];
                            long direct_referral_dwrank = 1;
                            using (MySqlConnection sql_conn = new MySqlConnection(conn))
                            {
                                sql_conn.Open(); // Open the connection
                                string dir_ref_sqlstr = $"SELECT COALESCE(max(setting_rank_id),1) FROM users WHERE deleted_at is null and role='user' and status = 'Active' and hierarchyList LIKE '%-{direct_referral_id}-%'; ";
                                //Console.WriteLine($"dir_ref_sqlstr: {dir_ref_sqlstr}");
                                MySqlCommand select_cmd = new MySqlCommand(dir_ref_sqlstr, sql_conn);
                                object result = select_cmd.ExecuteScalar();
                                if (result != null)
                                {
                                    dir_ref_info[2] = Convert.ToInt64(result);
                                    direct_referral_dwrank = (long)Convert.ToInt64(result);
                                }
                                dir_ref_info[3] = Math.Max(direct_referral_rank, direct_referral_dwrank);

                                // referrals which fulfill stored in string 
                                if ((long)dir_ref_info[3] == direct_referral_rank)
                                {
                                    lines_max_downlines = lines_max_downlines + $"|{direct_referral_id}:R{direct_referral_rank}";
                                }
                                else if ((long)dir_ref_info[3] == direct_referral_dwrank)
                                {
                                    dir_ref_sqlstr = $"SELECT id FROM users WHERE deleted_at is null and role='user' and status = 'Active' and hierarchyList LIKE '%-{direct_referral_id}-%' " +
                                                     $"and setting_rank_id = {direct_referral_dwrank} ORDER BY LENGTH(hierarchyList) asc limit 1; ";
                                    MySqlCommand select_cmd1 = new MySqlCommand(dir_ref_sqlstr, sql_conn);
                                    object result1 = select_cmd1.ExecuteScalar();
                                    if (result1 != null)
                                    {
                                        lines_max_downlines = lines_max_downlines + $"|{Convert.ToInt64(result1)}:R{direct_referral_dwrank}";
                                    }
                                }
                                Console.WriteLine($"dir_ref_info[3]: {dir_ref_info[3]}: lines_max_downlines: {lines_max_downlines}");
                            }
                        }

                        // sum up every lines and check rank and amount
                        var referralsCounts = direct_referral_List.GroupBy(user => (long)user[3]) // Group by the values in the second column
                                              .ToDictionary(group => group.Key, group => group.Count()); // Create a dictionary with counts

                        long rank_confirmed = min_rank_WOut_cultivate;
                        object[] fulfill_data = new object[4] { 0, 0, 0, "" };
                        long member_amt_confirmed = 0;
                        long target_member_confirmed = 0;

                        //Key - Referral Rank, Value - Count of Them
                        foreach (var ref_Count in referralsCounts.OrderByDescending(x => x.Key))
                        {
                            //Console.WriteLine($"Referral Rank : {ref_Count.Key}- Count: {ref_Count.Value}");
                            var cultivate_fulfill = cultivate_list.FirstOrDefault(rank => (long)rank[1] == (long)ref_Count.Key);
                            if (cultivate_fulfill != null)
                            {
                                //Console.WriteLine($"cultivate_fulfill : {cultivate_fulfill.Length}");
                                long cultivate_rank = (long)cultivate_fulfill[0];
                                long cultivate_type = (long)cultivate_fulfill[1];
                                long cultivate_member = (long)cultivate_fulfill[2];
                                //Console.WriteLine($"cultivate_type: {cultivate_type} - cultivate_member: {cultivate_member}");
                                //Console.WriteLine($"ref_Count.Key: {ref_Count.Key} - flag: {(ref_Count.Key == cultivate_type)}");
                                if ((long)ref_Count.Key == cultivate_type)
                                {
                                    //Console.WriteLine($"ref_Count.Value: {ref_Count.Value} >= cultivate_member: {cultivate_member}");
                                    if (ref_Count.Value >= cultivate_member)
                                    {
                                        rank_confirmed = cultivate_rank;
                                        member_amt_confirmed = (long)ref_Count.Value;
                                        target_member_confirmed = (long)ref_Count.Value;
                                        fulfill_data[0] = member_amt_confirmed;
                                        fulfill_data[1] = target_member_confirmed;
                                        fulfill_data[2] = cultivate_type;
                                        fulfill_data[3] = lines_max_downlines;
                                        break;
                                    }
                                }
                            }
                        }

                        object[] df_obj = user_list.FirstOrDefault(user => (long)user[0] == user_id);
                        if (df_obj != null)
                        {
                            //Console.WriteLine($" ****************  obj length {df_obj.Length}");
                            long user_rank = (long)df_obj[1];
                            //string dfString = "Before add DF List "+string.Join(", ", df_obj.Select(column => column.ToString())); //Console.WriteLine(dfString);
                            rank_confirmed = Math.Min((long)df_obj[11], rank_confirmed);
                            //Console.WriteLine($" **************** (long) df_obj[11]: {(long) df_obj[11]}- rank_confirmed: {rank_confirmed} - user_rank: {user_rank}");
                            // List<object[]> user_list,
                            //user_id, user_rank, personal_require, target_personal_require, package_req_rank,
                            //direct_referral, target_direct_referral, dir_referral_rank,
                            //group_sales, target_group_sales, group_sales_rank,
                            //least_rank, member_amt_confirmed, target_member_confirmed, cultivate_type, lines_max_downlines
                            if (rank_confirmed > user_rank)
                            {
                                df_obj[11] = rank_confirmed;
                                df_obj = df_obj.Concat(fulfill_data).ToArray();
                                //string fulfill_data_str = "fulfill_data "+string.Join(", ", fulfill_data.Select(column => column.ToString()));
                                //Console.WriteLine(fulfill_data_str);
                                //Console.WriteLine($" ****************  rank_confirmed: {rank_confirmed} -- user_rank: {user_rank} -- obj length {df_obj.Length}");
                                //string dfString = "DF List "+string.Join(", ", df_obj.Select(column => column.ToString()));
                                //Console.WriteLine(dfString);
                                insert_ranking_logs_update_user(df_obj);
                            }
                        }
                    }
                    else
                    {
                        // List<object[]> user_list,
                        //user_id, user_rank, personal_require, target_personal_require, package_req_rank,
                        //direct_referral, target_direct_referral, dir_referral_rank,
                        //group_sales, target_group_sales, group_sales_rank,
                        //least_rank 
                        //for no cultivate referral
                        var no_df_obj = user_list.FirstOrDefault(user => (long)user[0] == user_id);
                        if (no_df_obj != null)
                        {
                            long user_rank = (long)no_df_obj[1];
                            //user_rank = 1;
                            Console.WriteLine($"min_rank_WOut_cultivate: {min_rank_WOut_cultivate} | user_rank: {user_rank}"); 
                            
                            if (user_rank < min_rank_WOut_cultivate)
                            {
                                no_df_obj[11] = min_rank_WOut_cultivate;
                                //string nodfString = "No DF List " + string.Join(", ", no_df_obj.Select(column => column.ToString()));
                                //Console.WriteLine(nodfString); 
                                insert_ranking_logs_update_user(no_df_obj);
                            }
                        }
                    }
                }
            }
        }

        private static void proceed_summary_rebate(DateTime TimeNow)
        {
            var taskStopwatch = Stopwatch.StartNew();
            try
            {
                Console.WriteLine("proceed_summary_rebate ... ");
                string sqlstr = "select sub1.upline_id, sub1.upline_rank, sub1.upline_subs_id, sub1.downline_id, sub1.downline_rank, sub1.subs_id, sub1.meta_login, sub1.close_time, sub1.is_claimed, sub1.claimed_datetime, sum(sub1.trade_volume), sum(sub1.rebate_final_amt_get)  from " +
                                "(select upline_id, upline_rank, upline_subs_id, downline_id, downline_rank, subs_id, meta_login,  DATE(time_close) as close_time, is_claimed, claimed_datetime, trade_volume, rebate_final_amt_get " +
                                $"from trade_rebate_histories where deleted_at is null and is_claimed='Approved' and DATE(claimed_datetime) = '{TimeNow.ToString("yyyy-MM-dd")}') " +
                                $" as sub1 where sub1.subs_id not in (select COALESCE(subs_id, 0) from trade_rebate_summaries where deleted_at is null and DATE(execute_at) = '{TimeNow.ToString("yyyy-MM-dd")}' ) "+ 
                                "group by sub1.upline_id, sub1.upline_rank, sub1.upline_subs_id, sub1.downline_id, sub1.downline_rank, sub1.subs_id, sub1.meta_login, sub1.close_time,sub1.claimed_datetime;";

                Console.WriteLine($"proceed_summary_rebate - sqlstr: {sqlstr}");
                List<object[]> summary_List = new List<object[]>();
                using (MySqlConnection sql_conn = new MySqlConnection(conn))
                {
                    sql_conn.Open(); // Open the connection
                    MySqlCommand select_cmd = new MySqlCommand(sqlstr, sql_conn);
                    MySqlDataReader reader = select_cmd.ExecuteReader();
                    while (reader.Read())
                    {
                        // 0-upline_id, 1- upline_rank, 2- upline_subs_id， 3-downline_id, 4-downline_rank, 5-subs_id, 6-meta_login, 7-close_time, 8-status, 9-close_time, 10-trade_volume, 1`-rebate_final_amt_get
                        object[] summaryData = { reader.GetInt64(0), reader.GetInt64(1), reader.GetInt64(2), reader.GetInt64(3), reader.GetInt64(4), reader.GetInt64(5), reader.GetInt64(6),
                                                reader.GetDateTime(7), reader.GetString(8), reader.GetDateTime(9), reader.GetDouble(10), reader.GetDouble(11) };
                        summary_List.Add(summaryData);
                        //Console.WriteLine($"upline_id:{reader.GetInt64(0)}, downline_id:{reader.GetInt64(1)}, meta_login:{reader.GetInt64(2)}, sym_group_id:{reader.GetInt64(3)}, close_time:{reader.GetDateTime(4)}, status:{reader.GetString(5)}, trade_volume:{reader.GetInt64(6)}, rebate_final_amt_get:{reader.GetInt64(7)}");
                    }
                }

                using (MySqlConnection sql_conn = new MySqlConnection(conn))
                {
                    Console.WriteLine($" summary_List: {summary_List.Count}");
                    sql_conn.Open(); // Open the connection
                    foreach (var summary in summary_List)
                    {
                        long upline_id = (long)summary[0];
                        long upline_rank = (long)summary[1];
                        long upline_subsid = (long)summary[2];
                        long downline_id = (long)summary[3];
                        long downline_rank = (long)summary[4];
                        long subs_id = (long)summary[5];
                        long meta_login = (long)summary[6];
                        DateTime close_time = (DateTime)summary[7];
                        string claim_status = (string)summary[8];
                        DateTime claim_time = (DateTime)summary[9];
                        double trade_volume = (double)summary[10];
                        double rebate_amt = (double)summary[11];
                        long user_id = 0;

                        sqlstr = $"SELECT user_id FROM trading_accounts where deleted_at is null and meta_login = {meta_login};";
                        //Console.WriteLine($"select_cmd sqlstr: {sqlstr}");
                        MySqlCommand select_cmd = new MySqlCommand(sqlstr, sql_conn);
                        object result1 = select_cmd.ExecuteScalar();
                        if (result1 != null)
                        {
                            user_id = Convert.ToInt64(result1);
                        }
                        if(user_id > 0)
                        {    
                            string insert_sqlstr = $"INSERT INTO trade_rebate_summaries (upline_user_id, upline_rank, upline_subs_id, downline_id, downline_rank, subs_id, user_id, meta_login, closed_time, volume, rebate, status, execute_at, created_at ) VALUES " +
                                        $"({upline_id}, {upline_rank}, {upline_subsid}, {downline_id}, {downline_rank}, {subs_id}, {user_id}, {meta_login}, '{close_time.ToString("yyyy-MM-dd HH:mm:ss")}', ROUND({trade_volume},4), ROUND({rebate_amt},4), " +
                                        $"'Approved', '{claim_time.ToString("yyyy-MM-dd HH:mm:ss")}', '{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")}');";

                            Console.WriteLine($"insert_cmd insert_sqlstr: {insert_sqlstr}");
                            MySqlCommand insert_cmd = new MySqlCommand(insert_sqlstr, sql_conn);
                            insert_cmd.ExecuteScalar();
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"An exception occurred: {ex}");
            }
            taskStopwatch.Stop();
            Console.WriteLine("");
            Console.WriteLine($"Task proceed_summary_rebate completed in {taskStopwatch.Elapsed.TotalSeconds} seconds ({taskStopwatch.Elapsed.TotalMinutes})");
        }

        private static double retrieve_subs_id_quota(long subs_id)
        {
            double quota = 0;
            try
            {
                using (MySqlConnection sql_conn = new MySqlConnection(conn))
                {
                    sql_conn.Open(); // Open the connection
                    string sqlstr = $"SELECT COALESCE(cumulative_amount, 0) AS cumulative_amount, COALESCE(max_out_amount, 0) AS max_out_amount FROM subscriptions where deleted_at is null and id = {subs_id};";
                    //Console.WriteLine($"setting_rank_id, status -- sqlstr: {sqlstr} - user_id: {user_id}");  
                    MySqlCommand select_cmd = new MySqlCommand(sqlstr, sql_conn);
                    using (MySqlDataReader reader = select_cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            double cumulative_amount = (double) reader.GetDouble(0);
                            double max_out_amount = (double) reader.GetDouble(1);
                            quota = max_out_amount - cumulative_amount;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"An exception occurred: {ex}");
            }
            return quota;
        }

        private static void proceed_rebate_calculation(DateTime YTD_start, DateTime YTD_end)
        {
            var taskStopwatch = Stopwatch.StartNew();
            Console.WriteLine("proceed_rebate_calculation ... ");
            string sqlstr = "SELECT subscription_id, user_id, meta_login, volume, time_close, trade_profit FROM trade_histories where (rebate_status = 'Pending' or rebate_status = 'pending') and deleted_at is null " +
                            $"and master_id > 0 and time_close <= '{YTD_end.ToString("yyyy-MM-dd HH:mm:ss")}' and meta_login in (SELECT meta_login FROM trading_accounts where deleted_at is null and user_id in ( " +
                            $"SELECT id FROM users WHERE deleted_at is null and role='user' and status = 'Active' and (id = {lucky_ant_id} or top_leader_id = {lucky_ant_id}) or (top_leader_id in (570,572) ) )); ";   //and time_close >= {YTD_start.ToString("yyyy-MM-dd HH:mm:ss")}

            Console.WriteLine($"proceed_rebate_calculation - sqlstr: {sqlstr}");
            List<object[]> tradehist_List = new List<object[]>();
            using (MySqlConnection sql_conn = new MySqlConnection(conn))
            {
                sql_conn.Open(); // Open the connection
                MySqlCommand select_cmd = new MySqlCommand(sqlstr, sql_conn);
                MySqlDataReader reader = select_cmd.ExecuteReader();
                while (reader.Read())
                {
                    // subscription_id, user_id, meta_login, volume, time_close, trade_profit 
                    object[] tradeData = { reader.GetInt64(0), reader.GetInt64(1), reader.GetInt64(2), reader.GetDouble(3),
                                           reader.GetDateTime(4), reader.GetDouble(5)};
                    //Console.WriteLine($"meta_login:{reader.GetInt32(0)}, symbol:{reader.GetString(1)}, ticket:{reader.GetInt64(2)}, trade_type:{reader.GetString(3)}, time_open:{reader.GetDateTime(4).ToString("yyyy-MM-dd HH:mm:ss")}, time_close:{reader.GetDateTime(5).ToString("yyyy-MM-dd HH:mm:ss")}, volume:{reader.GetDouble(6)}");
                    tradehist_List.Add(tradeData);
                }
            }
            Console.WriteLine($"tradehist_List - {tradehist_List.Count}");

            foreach (var tradehist in tradehist_List)
            {
                long subscription_id = (long)tradehist[0];
                long user_id = (long)tradehist[1];
                long meta_login = (long)tradehist[2];
                double trade_volume = (double)tradehist[3];
                DateTime time_close = (DateTime)tradehist[4];
                double trade_pnl = (double)tradehist[5];

                long user_rank = 0; string user_hierlist = "";
                long user_active = 0; double rebate_perlot = 0; 

                Console.WriteLine("");
                retrieve_rebateInfo_based_metalogin(meta_login, user_id, ref user_rank, ref user_active, ref user_hierlist, ref rebate_perlot);

                Console.WriteLine($"meta_login:{meta_login}, time_close:{time_close.ToString("yyyy-MM-dd HH:mm:ss")}, volume:{trade_volume}");
                Console.WriteLine($"user_id:{user_id}, user_rank:{user_rank}, user_active:{user_active}, rebate_perlot:{rebate_perlot}, user_hierlist: {user_hierlist}");

                if (user_id > 0 && user_rank > 0 && user_active > 0)
                {
                    double subs_quota = retrieve_subs_id_quota(subscription_id);
                    double rebate_amount = trade_volume * rebate_perlot;
                    if (rebate_amount > 0 && subs_quota > 0)
                    {
                        // subscription_id, user_id, meta_login, volume, time_close, trade_profit 
                        if(subs_quota <= rebate_amount)
                        {
                            string remarks = $"[ {user_id}( R{user_rank}: {Math.Round(rebate_perlot,4)})] >> rebate owner ({Math.Round(rebate_perlot,4)} x {Math.Round(trade_volume,4)} = {Math.Round(rebate_amount,4)})";
                            insert_update_based_rebate(user_id, user_rank, subscription_id, meta_login, user_id, user_rank, subscription_id, subscription_id, meta_login, 
                                                    time_close, trade_volume, rebate_perlot, rebate_perlot, rebate_amount, subs_quota, subs_quota, remarks, 1);
                            
                            long bonus_wallet_id = 0;   double bonus_wallet_oldbal = 0;
                            long e_wallet_id = 0;   double e_wallet_oldbal = 0;
                            retrieve_bonus_e_wallet_data( user_id, ref bonus_wallet_id, ref bonus_wallet_oldbal, ref e_wallet_id, ref e_wallet_oldbal ); 
                            double bonus_precent_decimal = (double)bonus_precent_given / 100; double rewards_precent_decimal = (double)rewards_precent_given / 100;

                            // rebate divide 80% and 20%
                            double rebate_major = Math.Round(subs_quota * bonus_precent_decimal, 4); // Bonus Wallet
                            double rebate_minor = Math.Round(subs_quota * rewards_precent_decimal, 4); // E-wallets
                            Console.WriteLine($"rebate: {subs_quota} - bonus_precent_decimal: {bonus_precent_decimal} - rewards_precent_decimal: {rewards_precent_decimal}");

                            double new_bonus_wallet = bonus_wallet_oldbal + rebate_major; double new_e_wallet = e_wallet_oldbal + rebate_minor;
                            string remarks_major = $"LotSize Rebate (bonus_wallet) => ${Math.Round(subs_quota,4)} * {bonus_precent_given}% = ${Math.Round(rebate_major,4)}";
                            string remarks_minor = $"LotSize Rebate (e_wallet) => ${Math.Round(subs_quota, 4) } * {rewards_precent_given}% = ${Math.Round(rebate_minor,4) }"; 
                            insert_to_wallet_walletlog_transaction(0, user_id, subscription_id, bonus_wallet_id, bonus_wallet_oldbal, new_bonus_wallet, "bonus", "LotSizeRebate", rebate_major, remarks_major );   
                            insert_to_wallet_walletlog_transaction(1, user_id, subscription_id, e_wallet_id, e_wallet_oldbal, new_e_wallet, "bonus", "LotSizeRebate", rebate_minor,  remarks_minor );     
                        }    
                        else
                        {
                            string remarks = $"[ {user_id}( R{user_rank}: {Math.Round(rebate_perlot,4)})] >> rebate owner ({Math.Round(rebate_perlot,4)} x {Math.Round(trade_volume,4)} = {Math.Round(rebate_amount,4)})";
                            insert_update_based_rebate(user_id, user_rank, subscription_id, meta_login, user_id, user_rank, subscription_id, subscription_id, meta_login, 
                                                    time_close, trade_volume, rebate_perlot, rebate_perlot, rebate_amount, subs_quota, rebate_amount, remarks, 1);
                            
                            long bonus_wallet_id = 0;   double bonus_wallet_oldbal = 0;
                            long e_wallet_id = 0;   double e_wallet_oldbal = 0;
                            retrieve_bonus_e_wallet_data( user_id, ref bonus_wallet_id, ref bonus_wallet_oldbal, ref e_wallet_id, ref e_wallet_oldbal ); 
                            double bonus_precent_decimal = (double)bonus_precent_given / 100; double rewards_precent_decimal = (double)rewards_precent_given / 100;

                            // rebate divide 80% and 20%
                            double rebate_major = Math.Round(rebate_amount * bonus_precent_decimal, 4); // Bonus Wallet
                            double rebate_minor = Math.Round(rebate_amount * rewards_precent_decimal, 4); // E-wallets
                            Console.WriteLine($"rebate: {rebate_amount} - bonus_precent_decimal: {bonus_precent_decimal} - rewards_precent_decimal: {rewards_precent_decimal}");

                            double new_bonus_wallet = bonus_wallet_oldbal + rebate_major; double new_e_wallet = e_wallet_oldbal + rebate_minor;
                            string remarks_major = $"LotSize Rebate (bonus_wallet) => ${Math.Round(rebate_amount,4)} * {bonus_precent_given}% = ${Math.Round(rebate_major,4)}";
                            string remarks_minor = $"LotSize Rebate (e_wallet) => ${Math.Round(rebate_amount, 4) } * {rewards_precent_given}% = ${Math.Round(rebate_minor,4) }"; 
                            insert_to_wallet_walletlog_transaction(0, user_id, subscription_id, bonus_wallet_id, bonus_wallet_oldbal, new_bonus_wallet, "bonus", "LotSizeRebate", rebate_major, remarks_major );   
                            insert_to_wallet_walletlog_transaction(1, user_id, subscription_id, e_wallet_id, e_wallet_oldbal, new_e_wallet, "bonus", "LotSizeRebate", rebate_minor,  remarks_minor );  
                        }
                        override_to_uplines(user_id, user_rank, user_hierlist, subscription_id, meta_login, time_close, trade_volume, rebate_perlot);
                    }
                    else
                    {
                        if(subs_quota <= 0){   rebate_perlot = 0;  }
                        update_status_based_rebate(subscription_id);
                        override_to_uplines(user_id, user_rank, user_hierlist, subscription_id, meta_login, time_close, trade_volume, rebate_perlot);
                    }
                }
                // Deserialize each string back to object[]
                // Console.WriteLine($"Open Deal: {string.Join(", ", tradeAcc)}"); 
            }
            taskStopwatch.Stop();
            Console.WriteLine("");
            Console.WriteLine($"Task proceed_rebate_calculation completed in {taskStopwatch.Elapsed.TotalSeconds} seconds ({taskStopwatch.Elapsed.TotalMinutes})");
        }

        private static void override_to_uplines(long downline_userid, long downline_rank, string hierlist, long downline_sub_id, long downline_meta_login, 
                                                DateTime time_close, double trade_volume, double rebate_byrank)
        {
            if (hierlist != null && hierlist.Length > 0)
            {
                long dw_id = downline_userid;
                long dw_rank = downline_rank;
                long dw_sub_id = downline_sub_id;
                double dw_rebate_perlot = rebate_byrank;

                long subsid = downline_sub_id;
                long meta_login = downline_meta_login;

                string remark_hierlist = "";

                remark_hierlist = $"[ {dw_id} (R{dw_rank}: {dw_rebate_perlot})]";
                string[] hierlistSplit = hierlist.Split(new[] { '-' }, StringSplitOptions.RemoveEmptyEntries); ;
                for (int x = hierlistSplit.Length - 1; x >= 0; x--)
                {
                    if (hierlistSplit[x] != "")
                    {
                        long.TryParse(hierlistSplit[x], out long upline_id);
                        long upline_rank = 0; long upline_active = 0; double upline_rebate_perlot = 0;

                        retrieve_rebateInfo_based_userid(upline_id, ref upline_rank, ref upline_active, ref upline_rebate_perlot);
                        Console.WriteLine($"dw_id: {dw_id}, dw_rank: {dw_rank},  dw_rebate_perlot: {dw_rebate_perlot}");
                        Console.WriteLine($"upline_id: {upline_id}, upline_rank: {upline_rank} , upline_active: {upline_active}, upline_rebate_perlot: {upline_rebate_perlot}");
                        //Console.WriteLine($"upline_active: {upline_active} - upline_rank: {upline_rank} - dw_rank: {dw_rank} - upline_rebate_perlot: {upline_rebate_perlot} - dw_rebate_perlot: {upline_rebate_perlot}");
                        if (upline_active > 0 && upline_rank > 0 && upline_rank > dw_rank && upline_rebate_perlot > dw_rebate_perlot)
                        {
                            double net_rebate_lot = upline_rebate_perlot - dw_rebate_perlot;
                            double final_amount = net_rebate_lot * trade_volume;
                            string remarks = $"[ {upline_id} ( R{upline_rank}: {Math.Round(upline_rebate_perlot,4)})] - {remark_hierlist} = {Math.Round(net_rebate_lot,4)} x {Math.Round(trade_volume,4)} >> {Math.Round(final_amount,4)}";
                            
                            if (net_rebate_lot > 0)
                            {
                                List<object[]> subs_data_list = new List<object[]>();
                                retrieve_subscription_basedon_userid(upline_id, ref subs_data_list);
                                
                                Console.WriteLine($"Subs Count: {subs_data_list.Count }");

                                long upline_meta_login = 0;
                                long upline_sub_id = 0;

                                if(subs_data_list.Count > 0)
                                {
                                    double left_bal = final_amount ;

                                    foreach (var subs_data in subs_data_list)
                                    {
                                        upline_meta_login = (long) subs_data[5];
                                        upline_sub_id = (long) subs_data[0];
                                        double quota = (double)subs_data[3];
                                        
                                        if(quota > 0 &&  left_bal > 0)
                                        {
                                            if(quota >= left_bal) {  
                                                insert_update_based_rebate(upline_id, upline_rank, upline_sub_id, upline_meta_login, dw_id, dw_rank, dw_sub_id, subsid, meta_login, 
                                                           time_close, trade_volume, upline_rebate_perlot, net_rebate_lot, final_amount, quota, left_bal, remarks);


                                                long bonus_wallet_id = 0;   double bonus_wallet_oldbal = 0; long e_wallet_id = 0;   double e_wallet_oldbal = 0;
                                                retrieve_bonus_e_wallet_data( upline_id, ref bonus_wallet_id, ref bonus_wallet_oldbal, ref e_wallet_id, ref e_wallet_oldbal ); 
                                                double bonus_precent_decimal = (double)bonus_precent_given / 100; double rewards_precent_decimal = (double)rewards_precent_given / 100;

                                                // rebate divide 80% and 20%
                                                double rebate_major = Math.Round(left_bal * bonus_precent_decimal, 4); // Bonus Wallet
                                                double rebate_minor = Math.Round(left_bal * rewards_precent_decimal, 4); // E-wallets
                                                Console.WriteLine($"rebate: {left_bal} - bonus_precent_decimal: {bonus_precent_decimal} - rewards_precent_decimal: {rewards_precent_decimal}");

                                                double new_bonus_wallet = bonus_wallet_oldbal + rebate_major; double new_e_wallet = e_wallet_oldbal + rebate_minor;
                                                string remarks_major = $"LotSize Rebate (bonus_wallet) => ${Math.Round(left_bal,4)} * {bonus_precent_given}% = ${Math.Round(rebate_major,4)}";
                                                string remarks_minor = $"LotSize Rebate (e_wallet) => ${Math.Round(left_bal, 4) } * {rewards_precent_given}% = ${Math.Round(rebate_minor,4) }"; 
                                                insert_to_wallet_walletlog_transaction(0, upline_id, upline_sub_id, bonus_wallet_id, bonus_wallet_oldbal, new_bonus_wallet, "bonus", "LotSizeRebate", rebate_major, remarks_major );   
                                                insert_to_wallet_walletlog_transaction(1, upline_id, upline_sub_id, e_wallet_id, e_wallet_oldbal, new_e_wallet, "bonus", "LotSizeRebate", rebate_minor,  remarks_minor );     

                                                left_bal = left_bal - net_rebate_lot;

                                                
                                                //Console.WriteLine($" >= left_bal : {left_bal}");
                                            }
                                            else if (quota < left_bal){
                                                insert_update_based_rebate(upline_id, upline_rank, upline_sub_id, upline_meta_login, dw_id, dw_rank, dw_sub_id, subsid, meta_login, 
                                                           time_close, trade_volume, upline_rebate_perlot, net_rebate_lot, final_amount, quota, quota, remarks);
                                                
                                                long bonus_wallet_id = 0;   double bonus_wallet_oldbal = 0; long e_wallet_id = 0;   double e_wallet_oldbal = 0;
                                                retrieve_bonus_e_wallet_data( upline_id, ref bonus_wallet_id, ref bonus_wallet_oldbal, ref e_wallet_id, ref e_wallet_oldbal ); 
                                                double bonus_precent_decimal = (double)bonus_precent_given / 100; double rewards_precent_decimal = (double)rewards_precent_given / 100;

                                                // rebate divide 80% and 20%
                                                double rebate_major = Math.Round(quota * bonus_precent_decimal, 4); // Bonus Wallet
                                                double rebate_minor = Math.Round(quota * rewards_precent_decimal, 4); // E-wallets
                                                Console.WriteLine($"rebate: {quota} - bonus_precent_decimal: {bonus_precent_decimal} - rewards_precent_decimal: {rewards_precent_decimal}");

                                                double new_bonus_wallet = bonus_wallet_oldbal + rebate_major; double new_e_wallet = e_wallet_oldbal + rebate_minor;
                                                string remarks_major = $"LotSize Rebate (bonus_wallet) => ${Math.Round(quota,4)} * {bonus_precent_given}% = ${Math.Round(rebate_major,4)}";
                                                string remarks_minor = $"LotSize Rebate (e_wallet) => ${Math.Round(quota, 4) } * {rewards_precent_given}% = ${Math.Round(rebate_minor,4) }"; 
                                                insert_to_wallet_walletlog_transaction(0, upline_id, upline_sub_id, bonus_wallet_id, bonus_wallet_oldbal, new_bonus_wallet, "bonus", "LotSizeRebate", rebate_major, remarks_major );   
                                                insert_to_wallet_walletlog_transaction(1, upline_id, upline_sub_id, e_wallet_id, e_wallet_oldbal, new_e_wallet, "bonus", "LotSizeRebate", rebate_minor,  remarks_minor );     
                                                left_bal = left_bal - quota;
                                                    //Console.WriteLine($" < left_bal : {left_bal}");
                                            }
                                        }
                                    }
                                }

                                dw_id = upline_id;
                                dw_rank = upline_rank;
                                dw_sub_id = upline_sub_id;
                                dw_rebate_perlot = upline_rebate_perlot;
                            }
                        }
                        remark_hierlist = $" [ {upline_id} ( R{upline_rank}: {upline_rebate_perlot})] - {remark_hierlist}";
                    }
                }
            }
        }

        private static void update_status_based_rebate(long subscription_id)
        {
            using (MySqlConnection sql_conn = new MySqlConnection(conn))
            {
                sql_conn.Open(); // Open the connection

                string sqlstr = $"UPDATE trade_histories SET rebate_status = 'Approved', updated_at = '{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")}' WHERE subscription_id = {subscription_id} AND id > 0 ; ";
                Console.WriteLine($"update_status_based_rebate sqlstr: {sqlstr}");
                MySqlCommand update_cmd = new MySqlCommand(sqlstr, sql_conn);
                update_cmd.ExecuteScalar();
                //Console.WriteLine($"ConnectionString: {sql_conn.ConnectionTimeout}");
            }
        }

        private static void insert_update_based_rebate(long upline_userid, long upline_rank, long upline_sub_id, long upline_meta_login, 
                                                       long downline_userid, long downline_rank, long downline_sub_id, long sub_id, long meta_login, 
                                                       DateTime time_close, double trade_volume, double rebate_byrank, double net_rebate, double total_rebate, 
                                                       double subs_quota, double rebate_final_amt, string remarks, long update_flag = 0)
        {
            try
            {
                using (MySqlConnection sql_conn = new MySqlConnection(conn))
                {
                    sql_conn.Open(); // Open the connection
                    string sqlstr = $"INSERT INTO trade_rebate_histories( upline_id, upline_rank, upline_subs_id, upline_meta_login, downline_id, downline_rank, downline_subs_id, subs_id, meta_login, " +
                                    $"time_close, trade_volume, rebate_byrank, net_rebate_amt, total_rebate_amt, subs_quota, rebate_final_amt_get, is_claimed, claimed_datetime, remarks, created_at) " +
                                    $"VALUES ({upline_userid},{upline_rank}, {upline_sub_id}, {upline_meta_login}, {downline_userid}, {downline_rank}, {downline_sub_id}, {sub_id}, {meta_login}, " +
                                    $"'{time_close.ToString("yyyy-MM-dd HH:mm:ss")}', ROUND({trade_volume},4), ROUND({rebate_byrank},4), ROUND({net_rebate},4), ROUND({total_rebate},4), ROUND({subs_quota},4), " +
                                    $"ROUND({rebate_final_amt},4), 'Approved', '{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")}', '{remarks}', '{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")}');";
                    Console.WriteLine($"insert_cmd sqlstr: {sqlstr}");
                    MySqlCommand insert_cmd = new MySqlCommand(sqlstr, sql_conn);
                    insert_cmd.ExecuteScalar();

                    if (update_flag == 1)
                    {
                        sqlstr = $"UPDATE trade_histories SET rebate_status = 'Approved', updated_at = '{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")}' WHERE subscription_id = {sub_id} " +
                                 $"and user_id = {downline_userid} and rebate_status = 'Pending' and DATE(time_close) = DATE('{time_close.ToString("yyyy-MM-dd")}') and id > 0";
                        Console.WriteLine($"update_cmd sqlstr: {sqlstr}");
                        MySqlCommand update_cmd = new MySqlCommand(sqlstr, sql_conn);
                        update_cmd.ExecuteScalar();
                    }
                }
            }
            catch (Exception ex)    {   Console.WriteLine($"An exception occurred: {ex}");  }
        }

        private static void retrieve_userid(long user_id, ref long user_rank, ref long user_active, ref string user_role)
        {
            try
            {
                using (MySqlConnection sql_conn = new MySqlConnection(conn))
                {
                    sql_conn.Open(); // Open the connection
                    string sqlstr = $"SELECT setting_rank_id, status, role FROM users where deleted_at is null and id = {user_id};";
                    //Console.WriteLine($"setting_rank_id, status -- sqlstr: {sqlstr} - user_id: {user_id}");  
                    MySqlCommand select_cmd = new MySqlCommand(sqlstr, sql_conn);
                    using (MySqlDataReader reader = select_cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            string status = reader.GetString(1);
                            if (status == "Active") { user_active = 1; } else { user_active = 0; }
                            user_rank = (long)reader.GetInt64(0);
                            user_role = reader.GetString(2);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"An exception occurred: {ex}");
            }
        }

        private static void retrieve_rebateInfo_based_userid(long user_id, ref long user_rank, ref long user_active, ref double rebate_perlot)
        {
            try
            {
                using (MySqlConnection sql_conn = new MySqlConnection(conn))
                {
                    sql_conn.Open(); // Open the connection
                    string sqlstr = $"SELECT setting_rank_id, status FROM users where deleted_at is null and id = {user_id};";
                    //Console.WriteLine($"setting_rank_id, status -- sqlstr: {sqlstr} - user_id: {user_id}");  
                    MySqlCommand select_cmd = new MySqlCommand(sqlstr, sql_conn);
                    using (MySqlDataReader reader = select_cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            string status = reader.GetString(1);
                            if (status == "Active") { user_active = 1; } else { user_active = 0; }
                            user_rank = (long)reader.GetInt64(0);
                        }
                    }

                    if (user_rank > 0)
                    {
                        sqlstr = $"SELECT standard_lot FROM setting_ranks where deleted_at is null and id = {user_rank};";
                        //Console.WriteLine($"user_rank sqlstr: {sqlstr}"); 
                        select_cmd = new MySqlCommand(sqlstr, sql_conn);
                        object result1 = select_cmd.ExecuteScalar();
                        if (result1 != null)
                        {
                            rebate_perlot = Convert.ToDouble(result1);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"An exception occurred: {ex}");
            }
        }
        
        private static void retrieve_rebateInfo_based_metalogin(long login, long user_id, ref long user_rank, ref long user_active, ref string user_hierlist, ref double rebate_perlot)
        {
            try
            {
                using (MySqlConnection sql_conn = new MySqlConnection(conn))
                {
                    sql_conn.Open(); // Open the connection
                    if (user_id > 0)
                    {
                        string sqlstr = $"SELECT setting_rank_id, status, hierarchyList FROM users where deleted_at is null and id = {user_id};";
                        //Console.WriteLine($"setting_rank_id, status -- sqlstr: {sqlstr} - user_id: {user_id}");  
                        MySqlCommand select_cmd = new MySqlCommand(sqlstr, sql_conn);
                        using (MySqlDataReader reader = select_cmd.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                string status = reader.GetString(1);
                                if (status == "Active") { user_active = 1; } else { user_active = 0; }
                                user_rank = (long)reader.GetInt64(0);
                                user_hierlist = reader.IsDBNull(2) ? "" : reader.GetString(2);
                            }
                        }
                    }

                    if (user_rank > 0)
                    {
                        string sqlstr = $"SELECT standard_lot FROM setting_ranks where deleted_at is null and id = {user_rank};";
                        //Console.WriteLine($"user_rank sqlstr: {sqlstr}"); 
                        MySqlCommand select_cmd = new MySqlCommand(sqlstr, sql_conn);
                        object result1 = select_cmd.ExecuteScalar();
                        if (result1 != null) {  rebate_perlot = Convert.ToDouble(result1);  }
                    }
                }
            }
            catch (Exception ex)    {   Console.WriteLine($"An exception occurred: {ex}");  }
        }

        private static List<ulong> get_trading_accounts()
        {
            List<ulong> login_List = new List<ulong>();
            //login_List.Add(457284);
            string sqlstr = "SELECT meta_login FROM trading_accounts where deleted_at is null and balance > 0 and user_id in ( SELECT id FROM users WHERE deleted_at is null and role='user' and status = 'Active'  " +
                           $"and (id = {lucky_ant_id} or top_leader_id = {lucky_ant_id}) ) ;";

            Console.WriteLine($"insert_cmd sqlstr: {sqlstr}");

            using (MySqlConnection sql_conn = new MySqlConnection(conn))
            {
                sql_conn.Open(); // Open the connection
                MySqlCommand select_cmd = new MySqlCommand(sqlstr, sql_conn);
                MySqlDataReader reader = select_cmd.ExecuteReader();

                while (reader.Read())
                {
                    login_List.Add((ulong)reader.GetInt64(0));
                }
                //Console.WriteLine($"ConnectionString: {sql_conn.ConnectionTimeout}");
            }
            return login_List;
        }

        private static async Task<string> AwaitConsoleReadLine(int timeoutms)
        {
            Task<string> readLineTask = Task.Run(() => Console.ReadLine());

            if (await Task.WhenAny(readLineTask, Task.Delay(timeoutms)) == readLineTask)
            {
                return readLineTask.Result;
            }
            else
            {
                Console.WriteLine("Timeout!");
                return null;
            }
        }
    }
}
