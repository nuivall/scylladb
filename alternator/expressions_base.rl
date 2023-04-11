%%{
machine base;

action observe_path { observe_path(); }
action observe_path_root { observe_path_root(); }
action observe_path_dot { observe_path_dot(); }
action observe_path_index { observe_path_index(); }

# This start action is needed because value is a recursive type (can contain other values).
# It will be used to handle stack growth and has to be executed on start rather than end transition.
action observe_value_start { observe_value_start(); }
action observe_value_valref { observe_value_valref(); }
action observe_value_path { observe_value_path(); }

action observe_value_func_name { observe_value_func_name(); }
action observe_value_func_param { observe_value_func_param(); }

action mark_start { mark_start(fpc); }
action mark_end { mark_end(fpc); }

integer = digit+ >mark_start %mark_end;
name = (alpha ( alnum | '_' )* ) >mark_start %mark_end;
nameref = ('#' ( alnum | '_' )+) >mark_start %mark_end;
valref = (':' ( alnum | '_' )+ ) >mark_start %mark_end;
path_component = (name|nameref);
path = path_component %observe_path_root (
    ('.' path_component) %observe_path_dot
    | ('[' integer ']') %observe_path_index
)* %observe_path;

value = (
    valref %observe_value_valref
    | path %observe_value_path
    | name %observe_value_func_name '(' @{ fcall value_after_parenthesis; }
) >observe_value_start ;
value_after_parenthesis := (
    space* value %observe_value_func_param space*
    (',' space* value %observe_value_func_param space*)*
    ')' @{ fret; }
);

# Called on internal stack growth, we use static size as we want to limit it anyway.
prepush {
    if (_fsm_top >= _nesting_limit - 1) {
        throw expressions_syntax_error("Too many nesting levels found when parsing expression");
    }
}

}%%