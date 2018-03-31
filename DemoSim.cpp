/*
 * TranMgr.cpp
 *
 *  Created on: 14-Oct-2016
 *      Author: abhishek
 */
#include <Poco/Net/ServerSocket.h>
#include <Poco/Net/HTTPServer.h>
#include <Poco/Net/HTTPRequestHandler.h>
#include <Poco/Net/HTTPRequestHandlerFactory.h>
#include <Poco/Net/HTTPResponse.h>
#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/Util/ServerApplication.h>
#include <Poco/Net/HTMLForm.h>
#include <Poco/Net/PartHandler.h>
#include <Poco/CountingStream.h>
#include <Poco/NullStream.h>
#include <Poco/StreamCopier.h>

#include <Poco/Logger.h>
#include <Poco/FileChannel.h>
#include <Poco/AutoPtr.h>
#include <Poco/FormattingChannel.h>
#include <Poco/PatternFormatter.h>

#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <Poco/Dynamic/Var.h>

#include <Poco/NumberFormatter.h>

#include <Poco/Util/Option.h>
#include <Poco/Util/OptionSet.h>
#include <Poco/Util/HelpFormatter.h>

#include <Poco/NumberParser.h>

#include <iostream>
#include <string>
#include <vector>

#include <pqxx/pqxx>

#include <ap/Iso8583JSON.h>
#include <ap/constants.h>
#include <ap/DBManager.h>

using namespace Poco::Net;
using namespace Poco::Util;
using Poco::Net::NameValueCollection;
using namespace std;
using Poco::CountingInputStream;
using Poco::NullOutputStream;
using Poco::StreamCopier;

using Poco::Util::Option;
using Poco::Util::OptionSet;
using Poco::Util::OptionCallback;
using Poco::Util::HelpFormatter;
using Poco::Logger;
using Poco::FileChannel;
using Poco::AutoPtr;
using Poco::FormattingChannel;
using Poco::PatternFormatter;

using Poco::NumberFormatter;
using Poco::NumberParser;

using namespace Poco::JSON;
using namespace Poco::Dynamic;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////
int authid;
string dburl;
string hsmurl;
string env;

class DBUpdate : public Poco::Runnable {
public:

    DBUpdate(string modulename, Logger& alogger) :
    _modulename(modulename), m_logger(alogger) {
    }

    virtual void run() {
        try {
            DBManager dbm;

            pqxx::connection c(dburl);

            string query =
                    "UPDATE onl_process set last_update_datetime=now() where pname='"
                    + _modulename + "';";

            while (1) {
                pqxx::work txn(c);
                txn.exec(query);
                txn.commit();

                sleep(30);
            }
        } catch (Poco::Exception &e) {
            m_logger.error("Error while updating status in onl_process");
            m_logger.error(e.displayText());
            m_logger.error(e.message());
        }
    }

private:
    string _modulename;
    Logger& m_logger;
};

//////////////////////////////////////////////////////////////////////////////////////////////////////////////

class GetStatusHandler : public HTTPRequestHandler {
public:

    GetStatusHandler(Logger& alogger) :
    m_logger(alogger) {
    }

    ~GetStatusHandler() {
    }

    virtual void handleRequest(HTTPServerRequest &req, HTTPServerResponse &resp) {
        string responseStr = "OK";

        resp.setStatus(HTTPResponse::HTTP_OK);
        resp.setContentType("application/json; charset=UTF-8");

        ostream& out = resp.send();
        out << responseStr;

        out.flush();

        m_logger.information("Responded with OK");
    }

private:
    Logger& m_logger;
};

//////////////////////////////////////////////////////////////////////////////////////////////////////////////

class RequestHandler : public HTTPRequestHandler {
public:

    RequestHandler(Logger& logger) : m_logger(logger) {
    }

    ~RequestHandler() {
    }

    virtual void handleRequest(HTTPServerRequest &req,
            HTTPServerResponse &resp) {
        string responseStr;
        string requestStr;
        istream& istr = req.stream();

        StreamCopier::copyToString(istr, requestStr);

        responseStr = processMsg(requestStr);

        resp.setStatus(HTTPResponse::HTTP_OK);
        resp.setContentType("application/json");

        ostream& out = resp.send();
        out << responseStr;

        out.flush();
    }

private:
    Logger& m_logger;

    string processMsg(string request);
};

//////////////////////////////////////////////////////

string RequestHandler::processMsg(string request) {
    Iso8583JSON msg;

    string resp("NOK");

    try {
        msg.parseMsg(request);

        m_logger.information(msg.dumpMsg());

        string mti = msg.getMsgType();

        long amount = 0;

        if (msg.isFieldSet(_004_AMOUNT_TRANSACTION)) {
            amount = std::atol(msg.getField(_004_AMOUNT_TRANSACTION).c_str());
        }

        string tran_type = msg.getProcessingCode().getTrantype();
        if (tran_type.compare("31") == 0) {
            msg.setField(_038_AUTH_ID_RSP, "DEMO01");
            msg.setField(_039_RSP_CODE, "00");
            msg.setField(_054_ADDITIONAL_AMOUNTS, "0002356D0000001200001001356D000000120000");
        } else if (tran_type.compare("38") == 0) {
            msg.setField(_038_AUTH_ID_RSP, "DEMO01");
            msg.setField(_039_RSP_CODE, "00");
            msg.setField(_054_ADDITIONAL_AMOUNTS, "0002356D0000001200001001356D000000120000");
            msg.setField(_121_TRAN_DATA_RSP,"00100207002003GDN00500210006385Date        Amt  C/D     Descriptio15/09      10000.00C CDL ATM-[BHR] 15/09      10000.00C CDL ATM-[BHR] 15/09        500.00D WDL.ATM-[BHR] 15/09       1500.00D WDL.ATM-[BHR] 15/09       9500.00C CDL ATM-[BHR] 15/09       2000.00D WDL.ATM-[BHR] 15/09       4000.00D WDL.ATM-[BHR] 15/09       8700.00C CDL ATM-[BHR] 15/09       2000.00D WDL.ATM-[BHR] AVAIL BAL         000053792.08     ");
        } else if (amount > 0) {
            amount = std::atol(msg.getField(_004_AMOUNT_TRANSACTION).c_str());

            if (env.compare("P") == 0) {
                if (amount == 1234) {
                    msg.setField(_038_AUTH_ID_RSP, "DEMO01");
                    msg.setField(_039_RSP_CODE, "00");
                } else {
                    msg.setField(_039_RSP_CODE, "13");
                }
            } else {
                if (amount < 500) {
                    msg.setField(_039_RSP_CODE, "13");
                } else if (amount > 50000000) {
                    msg.setField(_039_RSP_CODE, "51");
                } else {
                    if (msg.isFieldSet(_055_EMV_DATA)) {
                        msg.setField(_055_EMV_DATA, "8A023030");
                    }
                    msg.setField(_038_AUTH_ID_RSP, "DEMO01");
                    msg.setField(_039_RSP_CODE, "00");
                }
            }
        } else {
            msg.setField(_038_AUTH_ID_RSP, "DEMO01");
            msg.setField(_039_RSP_CODE, "01");
        }

        msg.setRspMsgType();

        msg.removeField(_035_TRACK_2_DATA);
        msg.removeField(_045_TRACK_1_DATA);
        msg.removeField(_052_PIN_DATA);
        msg.removeField(_055_EMV_DATA);

        m_logger.information(msg.dumpMsg());

        resp = msg.toMsg();
    } catch (exception &e) {
        m_logger.error(Poco::format("Exception : (%s)", e.what()));

        msg.setRspMsgType();
        msg.setField(_039_RSP_CODE, _96_SYSTEM_MALFUNCTION);

        resp = msg.toMsg();
    }

    return resp;
}

//////////////////////////////////////////////////////

class RequestHandlerFactory : public HTTPRequestHandlerFactory {
public:

    RequestHandlerFactory(Logger& alogger) :
    m_logger(alogger) {
    }

    ~RequestHandlerFactory() {
    }

    virtual HTTPRequestHandler* createRequestHandler(
            const HTTPServerRequest& request) {
        if (request.getURI() == "/getstatus") {
            return new GetStatusHandler(m_logger);
        } else if (request.getURI() == "/updatestatus") {
            return new GetStatusHandler(m_logger);
        } else if (request.getURI() == "/transaction") {
            return new RequestHandler(m_logger);
        } else {
            return 0;
        }
    }

private:
    Logger& m_logger;
};

/////////////////////////////////////////////////////////////////////////////////////////

class DemoServerApp : public ServerApplication {
protected:

    void initialize(Application& self) {
        loadConfiguration();

        ServerApplication::initialize(self);
    }

    ////////////////////////////////////

    void uninitialize() {
        ServerApplication::uninitialize();
    }

    ////////////////////////////////////

    void defineOptions(OptionSet& options) {
        ServerApplication::defineOptions(options);

        options.addOption(
                Option("help", "h", "display argument help information").required(
                false).repeatable(false).callback(
                OptionCallback<DemoServerApp>(this,
                &DemoServerApp::handleHelp)));

        options.addOption(
                Option("config-file", "f", "load configuration data from a file")
                .required(false)
                .repeatable(true)
                .argument("file")
                .callback(OptionCallback<DemoServerApp>(this, &DemoServerApp::handleConfig)));
    }

    ////////////////////////////////////

    void handleHelp(const string& name, const string& value) {
        HelpFormatter helpFormatter(options());
        helpFormatter.setCommand(commandName());
        helpFormatter.setUsage("OPTIONS");
        helpFormatter.setHeader("Demo SIMULATOR (Prime)");
        helpFormatter.format(cout);
        stopOptionsProcessing();
        _helpRequested = true;
    }

    ////////////////////////////////////

    void handleConfig(const string& name, const string& value) {
        loadConfiguration(value);
    }

    ////////////////////////////////////

    int main(const vector<string> &) {
        if (!_helpRequested) {
            string mq_name = config().getString("mq_name", "NOK");
            string moduleName = config().getString("ModuleName", "NOK");
            string path = config().getString("path", "NOK");
            string rotation = config().getString("rotation", "NOK");
            string archive = config().getString("archive", "NOK");
            string ip = config().getString("IP", "127.0.0.1");
            string times = config().getString("times", "local");
            string compress = config().getString("compress", "true");
            string purgeAge = config().getString("purgeAge", "2 days");
            string purgeCount = config().getString("purgeCount", "2");
            string loglevel = config().getString("loglevel", "information");
            unsigned short port = (unsigned short) config().getInt("Port", 28080);
            unsigned short maxThreads = (unsigned short) config().getInt("MaxThreads", 100);
            unsigned short maxQueued = (unsigned short) config().getInt("MaxQueued", 100);
            unsigned short threadIdleTime = (unsigned short) config().getInt("ThreadIdleTime", 30);
            unsigned short maxConns = (unsigned short) config().getInt("MaxConns", 100);
            dburl = config().getString("DBURL");
            hsmurl = config().getString("HSMURL");
            env = config().getString("Environment", "P");

            AutoPtr<FileChannel> pChannel(new FileChannel);
            AutoPtr<PatternFormatter> pPF(new PatternFormatter);
            pPF->setProperty("pattern", "%Y-%m-%d %H:%M:%S %s: %t");
            pPF->setProperty("times", times);

            pChannel->setProperty("path", path);
            pChannel->setProperty("rotation", rotation);
            pChannel->setProperty("archive", archive);
            pChannel->setProperty("times", times);

            pChannel->setProperty("compress", compress);
            pChannel->setProperty("purgeAge", purgeAge);
            pChannel->setProperty("purgeCount", purgeCount);

            AutoPtr<FormattingChannel> pFC(
                    new FormattingChannel(pPF, pChannel));

            Logger::root().setChannel(pFC);

            Logger& logger = Logger::get(moduleName);
            if (loglevel.compare("fatal") == 0) {
                logger.setLevel(Poco::Message::PRIO_FATAL);
            } else if (loglevel.compare("critical") == 0) {
                logger.setLevel(Poco::Message::PRIO_CRITICAL);
            } else if (loglevel.compare("error") == 0) {
                logger.setLevel(Poco::Message::PRIO_ERROR);
            } else if (loglevel.compare("warning") == 0) {
                logger.setLevel(Poco::Message::PRIO_WARNING);
            } else if (loglevel.compare("notice") == 0) {
                logger.setLevel(Poco::Message::PRIO_NOTICE);
            } else if (loglevel.compare("information") == 0) {
                logger.setLevel(Poco::Message::PRIO_INFORMATION);
            } else if (loglevel.compare("debug") == 0) {
                logger.setLevel(Poco::Message::PRIO_DEBUG);
            } else if (loglevel.compare("trace") == 0) {
                logger.setLevel(Poco::Message::PRIO_TRACE);
            }

            logger.information("DemoSIM IP = " + ip + " / DemoSIM Port = "
                    + NumberFormatter::format(port));

            authid = 1;

            DBUpdate dbupd(moduleName, logger);
            Poco::Thread thread2;
            thread2.start(dbupd);

            HTTPServerParams* pParams = new HTTPServerParams;
            pParams->setKeepAlive(false);
            //pParams->setMaxThreads(maxThreads);
            pParams->setMaxQueued(maxQueued);
            //pParams->setThreadIdleTime(threadIdleTime);

            ServerSocket svs(port, maxConns);
            //svs.setReuseAddress(true);

            Poco::ThreadPool th(2, maxThreads, 60, 0);

            HTTPServer s(new RequestHandlerFactory(logger), th,
                    svs, pParams);

            s.start();

            logger.information("Server Started");

            waitForTerminationRequest(); // wait for CTRL-C or kill

            logger.information("Shutting down");
            s.stopAll(true); // Forcefully close all the connections
        }

        return Application::EXIT_OK;
    }

private:
    bool _helpRequested;
};

////////////////////////////////////////////////////////////////////////////////////////

int main(int argc, char** argv) {
    DemoServerApp app;

    return app.run(argc, argv);
}
