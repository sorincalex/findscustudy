
using ProtoBuf;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Xml;
using TRICS.General.Log;
using TRICS.Planner.Model.Common.Dicom;
using TRICS.Planner.Model.Common.Params;
using TRICS.Planner.Model.PatientData;
using TRICS.Wrapper.KeyValDB;
using TRICS.Wrapper.UnicodeToAscii;

namespace FindscuStudy {

    struct SupportedCharacterSets {
        public const string ASCII = "ISO_IR 6";
        public const string ASCIIEXT = "ISO 2022 IR 6";
        public const string UTF8 = "ISO_IR 192";
        public const string LATIN1 = "ISO_IR 100";
        public const string LATIN2 = "ISO_IR 101";
        public const string LATIN3 = "ISO_IR 109";
        public const string LATIN4 = "ISO_IR 110";
        public const string LATIN5 = "ISO_IR 148";
        public const string LATINEXT1 = "ISO 2022 IR 100";
        public const string LATINEXT2 = "ISO 2022 IR 101";
        public const string LATINEXT3 = "ISO 2022 IR 109";
        public const string LATINEXT4 = "ISO 2022 IR 110";
        public const string LATINEXT5 = "ISO 2022 IR 148";
    }

    class FindscuStudyRunner {
        
        private static KeyValDBClient dbClient = new KeyValDBClient();
        private static KeyValDBClient dbClient4 = new KeyValDBClient();
        private static KeyValDBClient dbClient2 = new KeyValDBClient();
        private static KeyValDBClient dbClient3 = new KeyValDBClient();

        private static int TIMEOUT = 60;
        private static string CALLING_DEFAULT = "FINDSCU";
        private static string CALLED_DEFAULT = "ANY-SCP";

        private string QRServerHost = "localhost";
        private int QRServerPort = 4242;
        private string QRServerAET = CALLED_DEFAULT;
        private string AET = CALLING_DEFAULT;
        private int timeout = TIMEOUT;

        private UnicodeToAscii u2a;
        private List<string> supportedAplhabets;

        public FindscuStudyRunner(string server, int port, string calling="FINDSCU", string called="ANY-SCP", int timeout=60) {
            QRServerHost = server;
            QRServerPort = port;
            if (!String.IsNullOrWhiteSpace(calling)) {
                AET = calling;
            }
            if (!String.IsNullOrWhiteSpace(called)) {
                QRServerAET = called;
            }
            this.timeout = timeout;
            u2a = new UnicodeToAscii("mappings.txtz");
            supportedAplhabets = new List<string>() {
                SupportedCharacterSets.ASCII,
                SupportedCharacterSets.ASCIIEXT,
                SupportedCharacterSets.LATIN1,
                SupportedCharacterSets.LATIN2,
                SupportedCharacterSets.LATIN3,
                SupportedCharacterSets.LATIN4,
                SupportedCharacterSets.LATIN5,
                SupportedCharacterSets.LATINEXT1,
                SupportedCharacterSets.LATINEXT2,
                SupportedCharacterSets.LATINEXT3,
                SupportedCharacterSets.LATINEXT4,
                SupportedCharacterSets.LATINEXT5,
                SupportedCharacterSets.UTF8
            };
        }

        public static void Init() {
            KeyValDBClient.Setup("127.0.0.1", 55123);
            dbClient4.Initialize("PatientsSummary");
            dbClient.Initialize("DicomPatientsData");
            dbClient2.Initialize("PatientsGPSFiles");
            dbClient3.Initialize("updates1");
        }
        
        public static bool Put(DicomPatientData person) {
            string key = person.PatientKey;
            bool ok = true;
            using (var outStream = new MemoryStream()) {
                Serializer.Serialize<DicomPatientData>(outStream, person);
                ok = dbClient.Update(key, outStream.ToArray());
            }
            if (ok) {
                dbClient3.Update(key, Encoding.ASCII.GetBytes("DicomPatientsData"));
                PatientGPSFileList gps = new PatientGPSFileList {
                    EntryTimestamp = DateTime.Now.Ticks
                };
                using (var outStream = new MemoryStream()) {
                    Serializer.Serialize<PatientGPSFileList>(outStream, gps);
                    ok &= dbClient2.Update(key, outStream.ToArray());
                }
                if (ok) {
                    dbClient3.Update(key, Encoding.ASCII.GetBytes("PatientsGPSFiles"));
                }
                return ok;
            }
            return false;
        }

        public static bool Put(Patient person) {
            string key = person.GetFallbackKey().ToUpper();
            bool ok = true;
            using (var outStream = new MemoryStream()) {
                Serializer.Serialize<Patient>(outStream, person);
                ok = dbClient4.Update(key, outStream.ToArray());
            }
            if (ok) {
                dbClient3.Update(key, Encoding.ASCII.GetBytes("PatientsSummary"));
                return ok;
            }
            return false;
        }

        public static DicomPatientData Get(string key) {
            byte[] toGet = dbClient.Get(key);
            using (var inStream = new MemoryStream(toGet)) {
                return Serializer.Deserialize<DicomPatientData>(inStream);
            }            
        }

        public static XmlDocument CreateXml(string com, string fileToConvert) {
            // Start the child process.
            Process p = new Process();
            // Redirect the output stream of the child process.
            p.StartInfo.UseShellExecute = false;
            p.StartInfo.RedirectStandardOutput = true;
            p.StartInfo.FileName = com;
            p.StartInfo.Arguments = fileToConvert;
            p.Start();
            // Do not wait for the child process to exit before
            // reading to the end of its redirected stream.
            // p.WaitForExit();
            // Read the output stream first and then wait.
            p.StandardOutput.ReadToEnd();
            p.WaitForExit();

            return null;
        }

        private List<DicomPatientData> ParseXml(string xmlfile, out List<Patient> patients) {
            var doc = new XmlDocument();
            doc.Load(xmlfile);
            var dicomPatients = new List<DicomPatientData>();
            patients = new List<Patient>();
            XmlNodeList nodes = doc.DocumentElement.SelectNodes("/responses/data-set");
            foreach (XmlNode node in nodes) {
                
                string patientFirstName = null, patientLastName = null, patientId = null, 
                    patientBirthDate = null, patientSex = null, characterSet = null;
                string transliteratedPatientFirstName = null, transliteratedPatientLastName = null;
                TimeParameter tp = null;
                TRICS.Planner.Model.Common.Gender gender = TRICS.Planner.Model.Common.Gender.Unknown;
                bool transliterationOk = false;

                foreach (XmlNode elem in node.SelectNodes("element")) {
                    var el = elem.Attributes.GetNamedItem("name");
                    if (el.InnerText.StartsWith("PatientName")) {
                        var patientName = elem.InnerText;
                        var group1 = patientName.Split('=')[0];
                        string converted = GetConvertedString(group1);
                        if (converted != null) {
                            transliterationOk = true;
                            var namePieces = group1.Split('^');
                            if (namePieces.Length == 1) {
                                // just the last name is given
                                patientLastName = namePieces[0];
                            }
                            if (namePieces.Length >= 2) {
                                patientLastName = namePieces[0];
                                patientFirstName = namePieces[1];
                            }

                            var transliteratedPieces = converted.Split('^');
                            if (transliteratedPieces.Length == 1) {
                                // just the last name is given
                                transliteratedPatientLastName = transliteratedPieces[0];
                            }
                            if (transliteratedPieces.Length >= 2) {
                                transliteratedPatientLastName = transliteratedPieces[0];
                                transliteratedPatientFirstName = transliteratedPieces[1];
                            }
                        }
                    } else if (el.InnerText.StartsWith("PatientID")) {
                        patientId = elem.InnerText;
                    } else if (el.InnerText.StartsWith("PatientBirthDate")) {
                        if (elem.InnerText.Length >= 8) {
                            
                            tp = new TimeParameter {
                                Year = Int32.Parse(elem.InnerText.Substring(0, 4)),
                                Month = Int32.Parse(elem.InnerText.Substring(4, 2)),
                                Day = Int32.Parse(elem.InnerText.Substring(6, 2))
                            };
                            
                            var year = Int32.Parse(elem.InnerText.Substring(0, 4));
                            var month = Int32.Parse(elem.InnerText.Substring(4, 2));
                            var day = Int32.Parse(elem.InnerText.Substring(6, 2));

                            patientBirthDate = day + "/" + month + "/" + year;
                        }
                    } else if (el.InnerText.StartsWith("PatientSex")) {
                        
                        if (String.IsNullOrWhiteSpace(elem.InnerText)) {
                            gender = TRICS.Planner.Model.Common.Gender.Unknown;
                        } else {
                            if (elem.InnerText.StartsWith("M")) {
                                gender = TRICS.Planner.Model.Common.Gender.Male;
                            } else if (elem.InnerText.StartsWith("F")) {
                                gender = TRICS.Planner.Model.Common.Gender.Female;
                            } else {
                                gender = TRICS.Planner.Model.Common.Gender.Any;
                            }
                        }

                        patientSex = elem.InnerText;
                        
                    } else if (el.InnerText.StartsWith("SpecificCharacterSet")) {
                        characterSet = elem.InnerText;
                    }
                }
                if (transliterationOk) {
                    // even if transliteration succeeds, check the character set

                    if (!String.IsNullOrWhiteSpace(characterSet)) {
                        // check if we support the first alphabet given
                        string alphabet = characterSet.Split('\\')[0].Trim();
                        if (alphabet.Count() > 0) {
                            if (!supportedAplhabets.Contains(alphabet)) {
                                // alphabet not supported, so we reject this patient altogheter
                                transliterationOk = false;
                            }
                        } // else it is the default ASCII
                    }
                    if (transliterationOk) {
                        Patient patient = new Patient {
                            ChartNumber = patientId,
                            LastName = transliteratedPatientLastName,
                            FirstName = transliteratedPatientFirstName,
                            Birthday = tp,
                            Gender = gender
                        };
                        patient.Birthday.UserFormat = "dd/MM/yyyy";
                        patients.Add(patient);

                        if (String.IsNullOrWhiteSpace(characterSet)) {
                            // in case we don't have a SpecificCharacterSet from findscu
                            // and the transliterated names don't coincide with the original ones,
                            // we need to specify UTF8 as character set, since the default ASCII won't do
                            if (
                                (
                                  (transliteratedPatientFirstName != null) &&
                                  !transliteratedPatientFirstName.Equals(patientFirstName, StringComparison.InvariantCultureIgnoreCase)
                                ) ||
                                (
                                  (transliteratedPatientLastName != null) &&
                                  !transliteratedPatientLastName.Equals(patientLastName, StringComparison.InvariantCultureIgnoreCase)
                                )
                            ) {
                                characterSet = SupportedCharacterSets.UTF8;
                            }
                        }

                        DicomPatientData person = new DicomPatientData {
                            Identity = new DicomWorkItem {
                                PatientLastName = patientLastName,
                                PatientFirstName = patientFirstName,
                                PatientID = patientId,
                                PatientBirthDate = patientBirthDate,
                                PatientSex = patientSex,
                                SpecificCharacterSet = characterSet
                            },
                            PatientKey = patient.GetFallbackKey().ToUpper()
                        };
                        dicomPatients.Add(person);
                    } else {
                        Logger.WriteMsg(LogSeverity.Warning, $"Given character set {characterSet} not supported");
                    }
                } else {
                    Logger.WriteMsg(LogSeverity.Warning, $"Can't transliterate patient with ID {patientId}");
                }
            }
            return dicomPatients;
        }

        public void ParseResponse(string modality) {
            string prms =
                QRServerHost + " " + QRServerPort;
            prms += (" --aetitle " + AET);
            prms += (" --call " + QRServerAET);
            prms += $" -S -k QueryRetrieveLevel=STUDY -k ModalitiesInStudy={modality} -k PatientName -k PatientID -k StudyInstanceUID -k StudyDate -k PatientBirthDate -k PatientSex --extract-xml-single out.xml";
            if (timeout > 0) {
                prms += (" -td " + timeout);
            }
            Logger.WriteMsg(LogSeverity.Info, $"Running findscu {prms}");
            CreateXml("findscu.exe", prms);

            List<DicomPatientData> persons = ParseXml("out.xml", out List<Patient> patients);
            Logger.WriteMsg(LogSeverity.Info, $"findscu retrieved {persons.Count} patients");

            for (int i = 0; i < patients.Count; i++) {

                DicomPatientData p = persons[i];
                Put(p);
                Put(patients[i]);
            }
            /*
            foreach (DicomPatientData p in persons) {
                DicomPatientData person = Get(p.PatientKey);
            }
            */
        }
        
        private string GetConvertedString(string value) {
            string convertedValue;

            if (u2a != null) {
                convertedValue = u2a.ConvertUnicodeString(value, out int unnmappedChars);

                if (unnmappedChars > 0) {
                    // can't transliterate
                    convertedValue = null;
                }
            } else {
                convertedValue = value;
            }

            return convertedValue;
        }

        static string GetLogFilePath(string destinationFolder, string fileRootName) {
            string folderPath = Path.GetFullPath(destinationFolder);

            if (!Directory.Exists(folderPath))
                Directory.CreateDirectory(folderPath);

            return Path.Combine(folderPath, fileRootName);
        }

        public static void Main(string[] args) {
            Logger.StartLogging(GetLogFilePath("Logs", "yati"), false);
            Logger.SetSeverityLevel(LogSeverity.Info);

            if (args.Length < 5) {
                Console.WriteLine("The following arguments need to be present:");
                Console.WriteLine(" - the IP or hostname of the Dicom server");
                Console.WriteLine(" - the port where the Dicom server is listening for connections");
                Console.WriteLine(" - the modality we are interested in (e.g. IOL, OAM, KER, OP)");
                Console.WriteLine(" - the name of the calling AE title");
                Console.WriteLine(" - the name of the called AE title");
                Console.WriteLine(" - (optional) timeout of the findscu operation");
                Environment.Exit(1);
            }

            string dicomHost = args[0];
            int dicomPort = Int16.Parse(args[1]);
            string modality = args[2];
            string callingAE = null;
            string calledAE = null;
            callingAE = args[3];
            calledAE = args[4];
            int timeout = TIMEOUT;
            if (args.Length > 5) {
                timeout = Int16.Parse(args[5]);
            }

            Init();

            var findscu = new FindscuStudyRunner(dicomHost, dicomPort, callingAE, calledAE, timeout);
            findscu.ParseResponse(modality);

        }

    }
}
