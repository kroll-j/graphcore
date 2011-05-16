#ifndef CLIBASE_H
#define CLIBASE_H

#ifndef _
#define _(string) gettext(string)
#endif


// convenience macros for printing success/failure/error messages from the cli.
// if the protocol for those messages should ever have to be modified, change these.
#define SUCCESS_STR "OK."
#define FAIL_STR "FAILED!"
#define ERROR_STR "ERROR!"
#define NONE_STR "NONE."
#define cliMessage(str, x...) ({ char c[2048]; int n= sprintf(c, str " "); snprintf(c+n, sizeof(c)-n, x); lastStatusMessage= c; })
#define cliSuccess(x...) cliMessage(SUCCESS_STR, x)
#define cliFailure(x...) cliMessage(FAIL_STR, x)
#define cliError(x...) cliMessage(ERROR_STR, x)
#define cliNone(x...) cliMessage(NONE_STR, x)

enum CommandStatus
{
    CMD_SUCCESS= 0,
    CMD_FAILURE,
    CMD_ERROR,
    CMD_NONE,
};



// abstract base class for cli commands
class CliCommand
{
    public:
        enum ReturnType
        {
            RT_NONE,
            RT_ARC_LIST,
            RT_NODE_LIST,
            RT_OTHER,
        };

        CliCommand() { }
        virtual ~CliCommand() { }

        // the command name
        virtual std::string getName()            { return "CliCommand"; }
        // one line describing the command and its parameters
        virtual std::string getSynopsis()        { return getName(); }
        // help text describing the function of the command
        virtual std::string getHelpText()        { return "Help text for " + getName() + "."; }
        void syntaxError()
        {
            lastStatusMessage= std::string(FAIL_STR) + _(" Syntax: ") + getSynopsis() + "\n";
            if(getReturnType()==RT_OTHER) std::cout << lastStatusMessage;
        }
        const std::string &getStatusMessage()    { return lastStatusMessage; }
        virtual ReturnType getReturnType()= 0;

        // read a data set of node IDs.
        // expectedSize: expected size of set per line (e. g. 1 for nodes, 2 for arcs)
        // update lastErrorString and return true on success, false on failure.
        bool readNodeset(FILE *inFile, std::vector< std::vector<uint32_t> > &dataset, unsigned expectedSize);

    protected:
        std::string lastStatusMessage;
};

#endif // CLIBASE_H
