#include "record.h"
#include <sstream>
#include <stdexcept>

static int safeStoi(const std::string &s) {
    return s.empty() ? 0 : std::stoi(s);
}

static double safeStod(const std::string &s) {
    return s.empty() ? 0.0 : std::stod(s);
}

Record Record::fromCSV(const std::string &line) {
    Record r;
    std::stringstream ss(line);
    std::string token;

    getline(ss, r.GAME_DATE_EST, '\t');
    getline(ss, token, '\t'); r.TEAM_ID_home   = safeStoi(token);
    getline(ss, token, '\t'); r.PTS_home       = safeStoi(token);
    getline(ss, token, '\t'); r.FG_PCT_home    = safeStod(token);
    getline(ss, token, '\t'); r.FT_PCT_home    = safeStod(token);
    getline(ss, token, '\t'); r.FG3_PCT_home   = safeStod(token);
    getline(ss, token, '\t'); r.AST_home       = safeStoi(token);
    getline(ss, token, '\t'); r.REB_home       = safeStoi(token);
    getline(ss, token, '\t'); r.HOME_TEAM_WINS = safeStoi(token);

    return r;
}


size_t Record::size() const {
    return sizeof(TEAM_ID_home) +
           sizeof(PTS_home) +
           sizeof(FG_PCT_home) +
           sizeof(FT_PCT_home) +
           sizeof(FG3_PCT_home) +
           sizeof(AST_home) +
           sizeof(REB_home) +
           sizeof(HOME_TEAM_WINS) +
           GAME_DATE_EST.size();
}

