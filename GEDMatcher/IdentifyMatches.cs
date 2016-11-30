using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace GEDMatcher
{
    class IdentifyMatches
    {
        static void Main(string[] args)
        {
            
            const double firstNameFactor = 1.0; 
            const double middleNameFactor = .2;
            const double suffixFactor = .1;
            const double SSNFactor = .1;
            const double addrFactor = .2;
            const double cityFactor = .01;
            const double stateFactor = 0;
            const double zipCodeFactor = .01;
            const double phoneFactor = .1;
            const double emailFactor = .1;
            const double threshold = 1.0;
            
            SqlConnection conn = new SqlConnection("Server=vulcan;database=MIS;Trusted_Connection=yes");

            try
            {
                conn.Open();
            }
            catch (Exception)
            {
                
                throw;
            }

            SqlCommand comm;
            SqlDataReader reader;
            StreamReader file = new StreamReader(args[0]);
            StreamWriter matches = new StreamWriter("..\\..\\..\\matches.csv");
            List<String> matchedSSNs = new List<string>();
            Dictionary<Tuple<String, String>, int> scores = new Dictionary<Tuple<string, string>, int>();
            Dictionary<String, String> studentInfo = new Dictionary<string, string>();
            Dictionary<String, DateTime> achievedDiploma = new Dictionary<string, DateTime>();

            file.ReadLine();

            while (!file.EndOfStream)
            {
                Regex re = new Regex("(?<=^|,)(\"(?:[^\"]|\"\")*\"|[^,]*)");

                List<String> column = new List<string>();

                MatchCollection m = re.Matches(file.ReadLine(), 0);

                String lastName = m[1].Value.Replace("'", "");
                String firstName = m[2].Value.Replace("'", "");
                String middleName = m[3].Value;
                String suffix = m[4].Value;
                String SSN = m[5].Value.Substring(0, m[5].Value.Length <= 4 ? 0 : 4);
                String DOB = m[6].Value;
                String gender = m[7].Value;
                String addr = m[10].Value;
                String city = m[12].Value;
                String state = m[13].Value;
                String zipCode = m[14].Value;
                String phone = m[15].Value;
                String email = m[16].Value;

                DateTime birthDate = DateTime.Parse(DOB);
                DOB = birthDate.ToString("yyyyMMdd");

                int LAScore = 0;
                int MAScore = 0;
                int SCScore = 0;
                int SSScore = 0;
                int totalScore;

                if (!String.IsNullOrEmpty(m[18].Value))
                {
                    LAScore = int.Parse(m[18].Value);
                }
                if (!String.IsNullOrEmpty(m[19].Value))
                {
                    MAScore = int.Parse(m[19].Value);
                }
                if (!String.IsNullOrEmpty(m[20].Value))
                {
                    SCScore = int.Parse(m[20].Value);
                }
                if (!String.IsNullOrEmpty(m[21].Value))
                {
                    SSScore = int.Parse(m[21].Value);
                }

                totalScore = int.Parse(m[22].Value);
                                                                                                          
                comm = new SqlCommand("SELECT                                                                                          "
                                      +" xwalk.PS_EMPL_ID                                                                              "
                                      +" ,stdnt.*,addr.*                                                                               "
                                      +" FROM                                                                                          "
                                      +"    MIS.dbo.ST_STDNT_A_125 stdnt                                                               "
                                      +"    INNER JOIN MIS.dbo.ST_ADDRESSES_A_153 addr ON addr.STUDENT_ID = stdnt.STUDENT_SSN          "
                                      +"    INNER JOIN MIS.dbo.ST_STDNT_SSN_SID_XWALK_606 xwalk ON xwalk.STUDENT_SSN = stdnt.STUDENT_ID"
                                      +" WHERE                                                                                         "
                                      +"    (stdnt.LST_NM = '" + lastName + "'                                                         "
                                      +"    OR stdnt.FRST_NM = '" + firstName + "')                                                    "
                                      +"    AND stdnt.DOB = '" + DOB + "'                                                              "
                                      +"    AND stdnt.SEX = '" + gender + "'", conn);

                reader = comm.ExecuteReader();

                String curSSN = "";
                bool matchFound = false;

                while (reader.Read())
                {
                    curSSN = reader["STUDENT_SSN"].ToString();

                    String dbLastName = reader["LST_NM"].ToString();
                    String dbFirstName = reader["FRST_NM"].ToString();
                    String dbMiddleName = reader["MDL_NM"].ToString();
                    String dbAddr = reader["STREET_1"].ToString();
                    String fscjEmail = reader["FCCJ_EMAIL_ADDR"].ToString();
                    String dbEmail = reader["EMAIL_ADDR"].ToString();
                    String dbphone = reader["HM_PHN"].ToString();
                    String workPhone = reader["WRK_PHN"].ToString();
                    String emplID = reader["PS_EMPL_ID"].ToString();

                    double firstNameCoeff = dbFirstName == firstName ? 1 : 1 / levenshtein(dbFirstName, firstName);
                    double middleNameCoeff =  dbMiddleName == middleName ? 1 : 1 / levenshtein(dbMiddleName, middleName);
                    double stateCoeff = reader["STATE"].ToString() == state ? 1 : 0;
                    double cityCoeff = reader["CITY"].ToString() == city ? 1 : 0;
                    double zipCoeff = reader["ZIP_CD"].ToString() == zipCode ? 1 : 0;
                    double addrCoeff = dbAddr == addr ? 1 : 1 / levenshtein(dbAddr, addr);
                    double phoneCoeff = GetNumbers(reader["HM_PHN"].ToString()) == phone ? 1 : 0;
                    double emailCoeff = email.ToUpper() == reader["EMAIL_ADDR"].ToString() ? 1 : 0;
                    double SSNCoeff = curSSN.Substring(0, 4) == SSN ? 1 : 0;
                    double suffixCoeff = reader["APPEND"].ToString() == suffix ? 1 : 0;

                    double match = firstNameCoeff * firstNameFactor + middleNameCoeff * middleNameFactor +
                        stateCoeff * stateFactor + cityCoeff * cityFactor + zipCoeff * zipCodeFactor +
                        addrCoeff * addrFactor + phoneCoeff * phoneFactor + emailCoeff * emailFactor +
                        SSNCoeff * SSNFactor + suffixCoeff * suffixFactor;

                    if (match > threshold)
                    {
                        if (!studentInfo.ContainsKey(curSSN))
                        {
                            studentInfo.Add(curSSN, curSSN + "," + emplID + "," + dbFirstName + "," + dbMiddleName + "," + lastName +
                                "," + birthDate.ToString("MM/dd/yyyy") + "," + fscjEmail + "," + dbEmail + "," + dbphone + "," + workPhone);
                        }

                        if (!achievedDiploma.ContainsKey(curSSN) && !String.IsNullOrEmpty(m[23].Value))
                        {
                            achievedDiploma.Add(curSSN, DateTime.Parse(m[23].Value));
                        }
                        matchFound = true;
                        break;
                    }
                }

                if (matchFound)
                {
                    if (LAScore > 0)
                    {
                        Tuple<String, String> key = new Tuple<string, string>(curSSN, "LA");

                        if (!scores.ContainsKey(key))
                        {
                            scores.Add(key, LAScore);
                        }
                        else if (LAScore > scores[key])
                        {
                            scores[key] = LAScore;
                        }
                    }
                    if (MAScore > 0)
                    {
                        Tuple<String, String> key = new Tuple<string, string>(curSSN, "MA");

                        if (!scores.ContainsKey(key))
                        {
                            scores.Add(key, MAScore);
                        }
                        else if (MAScore > scores[key])
                        {
                            scores[key] = MAScore;
                        }
                    }
                    if (SCScore > 0)
                    {
                        Tuple<String, String> key = new Tuple<string, string>(curSSN, "SC");

                        if (!scores.ContainsKey(key))
                        {
                            scores.Add(key, SCScore);
                        }
                        else if (SCScore > scores[key])
                        {
                            scores[key] = SCScore;
                        }
                    }
                    if (SSScore > 0)
                    {
                        Tuple<String, String> key = new Tuple<string, string>(curSSN, "SS");

                        if (!scores.ContainsKey(key))
                        {
                            scores.Add(key, SSScore);
                        }
                        else if (SSScore > scores[key])
                        {
                            scores[key] = SSScore;
                        }
                    }
                }

                reader.Close();
            }

            file.Close();

            String[] studentSSNs = new string[studentInfo.Keys.Count];
            studentInfo.Keys.CopyTo(studentSSNs, 0);

            matches.WriteLine("Student ID,EmplID,First Name,Middle Name,Last Name,Birth Date,Personal Email,FSCJ Email,"
                            +"Personal Email,Phone 1,Phone 2,Language Arts Score,Math Score,Science Score,Social Studies Score,Achieved Diploma Date");

            foreach (String SSN in studentSSNs)
            {
                Tuple<String, String> LAKey = new Tuple<string, string>(SSN, "LA");
                Tuple<String, String> MAKey = new Tuple<string, string>(SSN, "MA");
                Tuple<String, String> SCKey = new Tuple<string, string>(SSN, "SC");
                Tuple<String, String> SSKey = new Tuple<string, string>(SSN, "SS");

                String row = "";

                row += studentInfo[SSN];
                row += ",";
                row += scores.ContainsKey(LAKey) ? scores[LAKey] : 0;
                row += ",";
                row += scores.ContainsKey(MAKey) ? scores[MAKey] : 0;
                row += ",";
                row += scores.ContainsKey(SCKey) ? scores[SCKey] : 0;
                row += ",";
                row += scores.ContainsKey(SSKey) ? scores[SSKey] : 0;
                row += ",";
                row += achievedDiploma.ContainsKey(SSN) ? achievedDiploma[SSN].ToString("MM/dd/yyyy") : "";

                matches.WriteLine(row);
            }

            matches.Close();
        }

        //source: https://en.wikibooks.org/wiki/Algorithm_Implementation/Strings/Levenshtein_distance#C.23
        private static Int32 levenshtein(String a, String b)
        {

            if (string.IsNullOrEmpty(a))
            {
                if (!string.IsNullOrEmpty(b))
                {
                    return b.Length;
                }
                return 0;
            }

            if (string.IsNullOrEmpty(b))
            {
                if (!string.IsNullOrEmpty(a))
                {
                    return a.Length;
                }
                return 0;
            }

            Int32 cost;
            Int32[,] d = new int[a.Length + 1, b.Length + 1];
            Int32 min1;
            Int32 min2;
            Int32 min3;

            for (Int32 i = 0; i <= d.GetUpperBound(0); i += 1)
            {
                d[i, 0] = i;
            }

            for (Int32 i = 0; i <= d.GetUpperBound(1); i += 1)
            {
                d[0, i] = i;
            }

            for (Int32 i = 1; i <= d.GetUpperBound(0); i += 1)
            {
                for (Int32 j = 1; j <= d.GetUpperBound(1); j += 1)
                {
                    cost = Convert.ToInt32(!(a[i - 1] == b[j - 1]));

                    min1 = d[i - 1, j] + 1;
                    min2 = d[i, j - 1] + 1;
                    min3 = d[i - 1, j - 1] + cost;
                    d[i, j] = Math.Min(Math.Min(min1, min2), min3);
                }
            }

            return d[d.GetUpperBound(0), d.GetUpperBound(1)];

        }

        //source: http://stackoverflow.com/questions/3977497/stripping-out-non-numeric-characters-in-string
        private static string GetNumbers(string input)
        {
            return new string(input.Where(c => char.IsDigit(c)).ToArray());
        }
    }
}
