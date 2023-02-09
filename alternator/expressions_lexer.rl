#pragma once

#include <vector>
#include <string_view>

%%{
machine lexer;
access _fsm_;

name = alpha ( alnum | '_' );

main := |*
    # whitespace characters are ignored [ \t\v\f\n\r ]
    space;
    name => {yield_token(NAME);};
*|;
}%%

namespace alternator {

template<class tokenType>
class expressions_lexer  {
    int _fsm_cs;
    char* _fsm_ts;
    char* _fsm_te;

    // pointers to start and end of current token match
    char* _fsm_tokstart = nullptr;
    char* _fsm_tokend = nullptr;
    int _fsm_act = 0; // identity of the last pattern matched, used for backtracking

protected:
    std::string_view _buf;
    std::vector<TokenType> _tokens;

    void yield_token(int type) {
        _tokens.push_back(TokenType(type));
    }

public:
    expressions_lexer() {
        %% write init;
    }

    void parse(std::string_view buf) {
        _buf = buf
        // used as interface with generated code
        char* p = buf.data();
        char* pe = p + buf.size();
        char* eof = buf.empty() ? pe : nullptr;
        
        %% write exec;
    }
};

} // namespace alternator