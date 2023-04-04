%%{
machine base;

action observe_path_root { observe_path_root(); }
action observe_path_dot { observe_path_dot(); }
action observe_path_index { observe_path_index(); }


action observe_value_start { observe_value_start(); }
action observe_value_valref { observe_value_valref(); }
action observe_value_path { observe_value_path(); }

action observe_value_func_name { observe_value_func_name(); }
action observe_value_func_call { observe_value_func_call(); }
action observe_value_func_param { observe_value_func_param(); }

action mark_start {mark_start(fpc);}
action mark_end {mark_end(fpc);}

integer = digit+ >mark_start %mark_end;
name = (alpha ( alnum | '_' )* ) >mark_start %mark_end;
nameref = ('#' ( alnum | '_' )+) >mark_start %mark_end;
valref = (':' ( alnum | '_' )+ ) >mark_start %mark_end;
path_component = (name|nameref);
path = path_component %observe_path_root (
    ('.' path_component) %observe_path_dot
    | ('[' integer ']') %observe_path_index
)* ;

value = space* (
    valref %observe_value_valref
    | path %observe_value_path
    | name %observe_value_func_name '(' @{ fcall value_after_parenthesis; } %observe_value_func_call
) >observe_value_start space*;
value_after_parenthesis := (
    value %observe_value_func_param
    (',' value %observe_value_func_param)* 
    ')' @{ fret; }
);

prepush {
    if (_fsm_top >= _nesting_limit - 1) {
        throw expressions_syntax_error("Too many nesting levels found when parsing expression");
    }
}

}%%