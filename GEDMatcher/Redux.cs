using Microsoft.VisualBasic.FileIO;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GEDMatcher
{
    class Redux
    {
        public static void Main(String[] args)
        {

            const double firstNameWeight = .8;
            const double middleNameWeight = .2;
            const double suffixWeight = .1;
            const double SSNWeight = .1;
            const double addrWeight = .2;
            const double cityWeight = .01;
            const double stateWeight = 0;
            const double zipCodeWeight = .01;
            const double phoneWeight = .1;
            const double emailWeight = .1;
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
            StreamWriter matches = new StreamWriter("..\\..\\..\\matches.csv");
            List<String> matchedSSNs = new List<string>();
            Dictionary<Tuple<String, String>, int> scores = new Dictionary<Tuple<string, string>, int>();
            Dictionary<String, String> studentInfo = new Dictionary<string, string>();
            Dictionary<String, DateTime> achievedDiploma = new Dictionary<string, DateTime>();

            matches.WriteLine(@"Orion SSN,Orion StudentID,Orion FirstName,Orion LastName,Orion MiddleName,Gender,Orion Date of Birth,Orion Address,Orion City,Orion Phone,Diploma Date,First Name,Last Name,Middle Name,Date of Birth,State,City,ZipCode,Address,Email,SSN,Suffix,Gender,Match Confidence", conn);

            { // Parse File and Build and Upload Data Table to be matched against Orion
                using (TextFieldParser parser = new TextFieldParser(args[0]))
                {
                    parser.TextFieldType = FieldType.Delimited;
                    parser.SetDelimiters(",");
                    parser.HasFieldsEnclosedInQuotes = true;

                    List<String> fields = new List<string>(parser.ReadFields());

                    DataTable table = new DataTable("ProspectiveGEDStudents");
                    DataColumn column;
                    DataRow row;

                    column = new DataColumn();
                    column.DataType = Type.GetType("System.String");
                    column.ColumnName = "FirstName";
                    table.Columns.Add(column);

                    column = new DataColumn();
                    column.DataType = Type.GetType("System.String");
                    column.ColumnName = "LastName";
                    table.Columns.Add(column);

                    column = new DataColumn();
                    column.DataType = Type.GetType("System.String");
                    column.ColumnName = "Middle";
                    table.Columns.Add(column);

                    column = new DataColumn();
                    column.DataType = Type.GetType("System.String");
                    column.ColumnName = "Suffix";
                    table.Columns.Add(column);

                    column = new DataColumn();
                    column.DataType = Type.GetType("System.String");
                    column.ColumnName = "PartialSSN";
                    table.Columns.Add(column);

                    column = new DataColumn();
                    column.DataType = Type.GetType("System.String");
                    column.ColumnName = "Gender";
                    table.Columns.Add(column);

                    column = new DataColumn();
                    column.DataType = Type.GetType("System.String");
                    column.ColumnName = "Race";
                    table.Columns.Add(column);

                    column = new DataColumn();
                    column.DataType = Type.GetType("System.String");
                    column.ColumnName = "HighestGrade";
                    table.Columns.Add(column);

                    column = new DataColumn();
                    column.DataType = Type.GetType("System.String");
                    column.ColumnName = "DateOfBirth";
                    table.Columns.Add(column);

                    column = new DataColumn();
                    column.DataType = Type.GetType("System.String");
                    column.ColumnName = "Addr1";
                    table.Columns.Add(column);

                    column = new DataColumn();
                    column.DataType = Type.GetType("System.String");
                    column.ColumnName = "Addr2";
                    table.Columns.Add(column);

                    column = new DataColumn();
                    column.DataType = Type.GetType("System.String");
                    column.ColumnName = "City";
                    table.Columns.Add(column);

                    column = new DataColumn();
                    column.DataType = Type.GetType("System.String");
                    column.ColumnName = "State";
                    table.Columns.Add(column);

                    column = new DataColumn();
                    column.DataType = Type.GetType("System.String");
                    column.ColumnName = "Zip";
                    table.Columns.Add(column);

                    column = new DataColumn();
                    column.DataType = Type.GetType("System.String");
                    column.ColumnName = "Phone";
                    table.Columns.Add(column);

                    column = new DataColumn();
                    column.DataType = Type.GetType("System.String");
                    column.ColumnName = "Email";
                    table.Columns.Add(column);

                    column = new DataColumn();
                    column.DataType = Type.GetType("System.Int32");
                    column.ColumnName = "ReadingScore";
                    column.DefaultValue = 0;
                    table.Columns.Add(column);

                    column = new DataColumn();
                    column.DataType = Type.GetType("System.Int32");
                    column.ColumnName = "WritingScore";
                    column.DefaultValue = 0;
                    table.Columns.Add(column);

                    column = new DataColumn();
                    column.DataType = Type.GetType("System.Int32");
                    column.ColumnName = "LanguageArtsScore";
                    column.DefaultValue = 0;
                    table.Columns.Add(column);

                    column = new DataColumn();
                    column.DataType = Type.GetType("System.Int32");
                    column.ColumnName = "MathScore";
                    column.DefaultValue = 0;
                    table.Columns.Add(column);

                    column = new DataColumn();
                    column.DataType = Type.GetType("System.Int32");
                    column.ColumnName = "SocialStudiesScore";
                    column.DefaultValue = 0;
                    table.Columns.Add(column);

                    column = new DataColumn();
                    column.DataType = Type.GetType("System.Int32");
                    column.ColumnName = "ScienceScore";
                    column.DefaultValue = 0;
                    table.Columns.Add(column);

                    column = new DataColumn();
                    column.DataType = Type.GetType("System.Int32");
                    column.ColumnName = "TotalScore";
                    column.DefaultValue = 0;
                    table.Columns.Add(column);

                    column = new DataColumn();
                    column.DataType = Type.GetType("System.DateTime");
                    column.ColumnName = "DiplomaDate";
                    table.Columns.Add(column);
                    
                    while (!parser.EndOfData)
                    {
                        String[] studentRow = parser.ReadFields();

                        String lastName = studentRow[1].Replace("'", "");
                        String firstName = studentRow[2].Replace("'", "");
                        String middleName = studentRow[3];
                        String suffix = studentRow[4];
                        String SSN = studentRow[5].Substring(0, studentRow[5].Length <= 4 ? 0 : 4);
                        String DOB = studentRow[6];
                        String gender = studentRow[7].Substring(0, 1);
                        String race = studentRow[8];
                        String addr = studentRow[10];
                        String addr2 = studentRow[11];
                        String city = studentRow[12];
                        String state = studentRow[13];
                        String zipCode = studentRow[14];
                        String phone = studentRow[15];
                        String email = studentRow[16];

                        DateTime birthDate = DateTime.Parse(DOB);
                        DOB = birthDate.ToString("yyyyMMdd");

                        if (table.Select("FirstName = '" + firstName + "' AND LastName = '" + lastName + "' AND PartialSSN = '" + SSN + "'").Length == 0)
                        {
                            row = table.NewRow();
                            row["FirstName"] = firstName;
                            row["LastName"] = lastName;
                            row["Middle"] = middleName;
                            row["Suffix"] = suffix;
                            row["PartialSSN"] = SSN;
                            row["Gender"] = gender;
                            row["DateOfBirth"] = DOB;
                            row["Addr1"] = addr;
                            row["City"] = city;
                            row["State"] = state;
                            row["Phone"] = phone;
                            row["Email"] = email;
                            row["Race"] = race;
                            row["Zip"] = zipCode;
                            table.Rows.Add(row);
                        }
                        else
                        {
                            row = table.Select("FirstName = '" + firstName + "' AND LastName = '" + lastName + "' AND PartialSSN = '" + SSN + "'")[0];
                        }

                        //int REScore = String.IsNullOrEmpty(studentRow[18]) ? 0 : int.Parse(studentRow[18]);
                        //int WRScore = String.IsNullOrEmpty(studentRow[19]) ? 0 : int.Parse(studentRow[19]);
                        int LAScore = String.IsNullOrEmpty(studentRow[18]) ? 0 : int.Parse(studentRow[18]);
                        int MAScore = String.IsNullOrEmpty(studentRow[19]) ? 0 : int.Parse(studentRow[19]);
                        int SCScore = String.IsNullOrEmpty(studentRow[20]) ? 0 : int.Parse(studentRow[20]);
                        int SSScore = String.IsNullOrEmpty(studentRow[21]) ? 0 : int.Parse(studentRow[21]);
                        int totalScore = int.Parse(studentRow[22]);

                        //if (REScore > 0 || REScore > (int)row["ReadingScore"])
                        //{
                        //    row["ReadingScore"] = REScore;
                        //}
                        //if (WRScore > 0 || WRScore > (int)row["WritingScore"])
                        //{
                        //    row["WritingScore"] = WRScore;
                        //}
                        if (LAScore > 0 || LAScore > (int)row["LanguageArtsScore"])
                        {
                            row["LanguageArtsScore"] = LAScore;
                        }
                        if (MAScore > 0 || MAScore > (int)row["MathScore"])
                        {
                            row["MathScore"] = MAScore;
                        }
                        if (SCScore > 0 || SCScore > (int)row["ScienceScore"])
                        {
                            row["ScienceScore"] = SCScore;
                        }
                        if (SSScore > 0 || SSScore > (int)row["SocialStudiesScore"])
                        {
                            row["SocialStudiesScore"] = SSScore;
                        }
                        if (totalScore > 0 || totalScore > (int)row["TotalScore"])
                        {
                            row["TotalScore"] = totalScore;
                        }
                        if (!String.IsNullOrEmpty(studentRow[23]))
                        {
                            row["DiplomaDate"] = DateTime.Parse(studentRow[23]);
                        }
                    }

                    SqlBulkCopy bulkCopy = new SqlBulkCopy(conn);
                    bulkCopy.DestinationTableName = "VFA.dbo.ProspectiveGEDStudents";

                    SqlBulkCopyColumnMappingCollection coll = bulkCopy.ColumnMappings;

                    coll.Add(new SqlBulkCopyColumnMapping("FirstName", "FirstName"));
                    coll.Add(new SqlBulkCopyColumnMapping("LastName", "LastName"));
                    coll.Add(new SqlBulkCopyColumnMapping("Middle", "Middle"));
                    coll.Add(new SqlBulkCopyColumnMapping("Suffix", "Suffix"));
                    coll.Add(new SqlBulkCopyColumnMapping("PartialSSN", "PartialSSN"));
                    coll.Add(new SqlBulkCopyColumnMapping("Gender", "Gender"));
                    coll.Add(new SqlBulkCopyColumnMapping("Race", "Race"));
                    coll.Add(new SqlBulkCopyColumnMapping("HighestGrade", "HighestGrade"));
                    coll.Add(new SqlBulkCopyColumnMapping("DateOfBirth", "DateOfBirth"));
                    coll.Add(new SqlBulkCopyColumnMapping("Addr1", "Addr1"));
                    coll.Add(new SqlBulkCopyColumnMapping("Addr2", "Addr2"));
                    coll.Add(new SqlBulkCopyColumnMapping("City", "City"));
                    coll.Add(new SqlBulkCopyColumnMapping("Zip", "Zip"));
                    coll.Add(new SqlBulkCopyColumnMapping("State", "State"));
                    coll.Add(new SqlBulkCopyColumnMapping("Phone", "Phone"));
                    coll.Add(new SqlBulkCopyColumnMapping("Email", "Email"));
                    coll.Add(new SqlBulkCopyColumnMapping("ReadingScore", "ReadingScore"));
                    coll.Add(new SqlBulkCopyColumnMapping("WritingScore", "WritingScore"));
                    coll.Add(new SqlBulkCopyColumnMapping("LanguageArtsScore", "LanguageArtsScore"));
                    coll.Add(new SqlBulkCopyColumnMapping("MathScore", "MathScore"));
                    coll.Add(new SqlBulkCopyColumnMapping("ScienceScore", "ScienceScore"));
                    coll.Add(new SqlBulkCopyColumnMapping("SocialStudiesScore", "SocialStudiesScore"));
                    coll.Add(new SqlBulkCopyColumnMapping("TotalScore", "TotalScore"));
                    coll.Add(new SqlBulkCopyColumnMapping("DiplomaDate", "DiplomaDate"));
                    bulkCopy.WriteToServer(table);
                    bulkCopy.Close();

                    
                }
            }

            {//Match students against ORION

                comm = new SqlCommand(@"SELECT DISTINCT
	                                        xwalk.STUDENT_ID,
	                                        stdnt.STUDENT_SSN,
	                                        stdnt.FRST_NM,
	                                        stdnt.LST_NM,
	                                        stdnt.MDL_NM,
	                                        addr.STREET_1,
                                            addr.ZIP_CD,
	                                        addr.CITY AS [ORION CITY],
                                            addr.STATE AS [ORION STATE],
	                                        stdnt.FCCJ_EMAIL_ADDR,
	                                        stdnt.EMAIL_ADDR,
	                                        stdnt.HM_PHN,
	                                        stdnt.WRK_PHN,
	                                        stdnt.DOB,
                                            stdnt.APPEND,
                                            stdnt.SEX,
	                                        ged.*
                                        FROM 
	                                        VFA.[dbo].[ProspectiveGEDStudents] ged
	                                        INNER JOIN MIS.dbo.ST_STDNT_A_125 stdnt ON (stdnt.FRST_NM = ged.FirstName
											                                        OR stdnt.LST_NM = ged.LastName)
											                                        AND stdnt.DOB = ged.DateOfBirth
											                                        AND stdnt.SEX = ged.Gender
	                                        INNER JOIN MIS.dbo.ST_ADDRESSES_A_153 addr ON addr.STUDENT_ID = stdnt.STUDENT_SSN
	                                        INNER JOIN MIS.dbo.ST_STDNT_SSN_SID_XWALK_606 xwalk ON xwalk.STUDENT_SSN = stdnt.STUDENT_SSN", conn);

                reader = comm.ExecuteReader();
                

                while (reader.Read())
                {
                    String orSSN = reader["STUDENT_SSN"].ToString();
                    String orStudentID = reader["STUDENT_ID"].ToString();
                    String orLastName = reader["LST_NM"].ToString();
                    String orFirstName = reader["FRST_NM"].ToString();
                    String orMiddleName = reader["MDL_NM"].ToString();
                    String orAddr = reader["STREET_1"].ToString();
                    String orCity = reader["CITY"].ToString();
                    String orfscjEmail = reader["FCCJ_EMAIL_ADDR"].ToString();
                    String orEmail = reader["EMAIL_ADDR"].ToString();
                    String orphone = new String(reader["HM_PHN"].ToString().Where(ch => char.IsDigit(ch)).ToArray());
                    String orworkPhone = new String(reader["WRK_PHN"].ToString().Where(ch => char.IsDigit(ch)).ToArray());
                    String orGender = reader["SEX"].ToString();
                    String orSuffix = reader["APPEND"].ToString();
                    String orState = reader["ORION STATE"].ToString().ToUpper();
                    String orZIP = reader["ZIP_CD"].ToString();

                    String orDOB = DateTime.ParseExact(reader["DOB"].ToString(), "yyyyMMdd", CultureInfo.InvariantCulture)
                        .ToString("MM/dd/yyyy");

                    String lastName = reader["LastName"].ToString().ToUpper();
                    String firstName = reader["FirstName"].ToString().ToUpper();
                    String middleName = reader["Middle"].ToString().ToUpper();
                    String state = reader["State"].ToString().ToUpper();
                    String city = reader["City"].ToString().ToUpper();
                    String zipCode = reader["Zip"].ToString();
                    String addr = reader["Addr1"].ToString().ToUpper();
                    String phone = new String(reader["Phone"].ToString().Where(ch => char.IsDigit(ch)).ToArray());
                    String email = reader["Email"].ToString().ToUpper();
                    String SSN = reader["PartialSSN"].ToString();
                    String suffix = reader["Suffix"].ToString().ToUpper();
                    String gender = reader["Gender"].ToString().ToUpper() ;

                    String dob = DateTime.ParseExact(reader["DateOfBirth"].ToString(), "yyyyMMdd", CultureInfo.InvariantCulture)
                        .ToString("MM/dd/yyyy");

                    int readingScore = int.Parse(reader["ReadingScore"].ToString());
                    int writingScore = int.Parse(reader["WritingScore"].ToString());
                    int languageArtsScore = int.Parse(reader["LanguageArtsScore"].ToString());
                    int mathScore = int.Parse(reader["MathScore"].ToString());
                    int socialStudiesScore = int.Parse(reader["SocialStudiesScore"].ToString());
                    int scienceScore = int.Parse(reader["ScienceScore"].ToString());
                    int totalScore = int.Parse(reader["TotalScore"].ToString());

                    DateTime diplomaDate;
                    String diplomaDateString = "";

                    if (DateTime.TryParse(reader["DiplomaDate"].ToString(), out diplomaDate))
                    {
                        diplomaDateString = diplomaDate.ToString("MM/dd/yyyy");
                    }

                    double firstNameFactor = orFirstName == firstName ? 1 : 1 / levenshtein(orFirstName, firstName);
                    double middleNameFactor = orMiddleName == middleName ? 1 : 1 / levenshtein(orMiddleName, middleName);
                    double stateFactor = orState == state ? 1 : 0;
                    double cityFactor = orCity == city ? 1 : 1 / levenshtein(orCity, city);
                    double zipFactor = orZIP == zipCode ? 1 : 0;
                    double addrFactor = orAddr == addr ? 1 : 1 / levenshtein(orAddr, addr);
                    double phoneFactor = orphone == phone ? 1 : 0;
                    double emailFactor = email.ToUpper() == orEmail ? 1 : 0;
                    double SSNFactor = orSSN.Substring(0, 4) == SSN ? 1 : 0;
                    double suffixFactor = orSuffix == suffix ? 1 : 1 / levenshtein(orSuffix, suffix);

                    double match = firstNameFactor * firstNameWeight + middleNameFactor * middleNameWeight +
                        stateFactor * stateWeight + cityFactor * cityWeight + zipFactor * zipCodeWeight +
                        addrFactor * addrWeight + phoneFactor * phoneWeight + emailFactor * emailWeight +
                        SSNFactor * SSNWeight + suffixFactor * suffixWeight;

                    if (!(firstName == orFirstName && middleName == orMiddleName && dob == orDOB && gender == orGender))
                    {
                        orStudentID = "";
                    }

                    if (match > threshold && !matchedSSNs.Contains(orSSN))
                    {
                        matches.WriteLine("=" + String.Join(",", ( new String[] { orSSN, orStudentID,  orFirstName, orLastName, orMiddleName, gender,
                            orDOB, orAddr, orCity, orphone, diplomaDateString, firstName, lastName, middleName, dob, state, city, zipCode, addr, email, SSN + "*****",
                            suffix, gender, match.ToString() } ).Select(s => @"""" + s + @"""" )));
                        
                        matchedSSNs.Add(orSSN);
                    }
                }
            }
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
    }
}
