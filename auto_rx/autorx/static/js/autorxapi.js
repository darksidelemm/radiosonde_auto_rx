// auto_rx API Helpers

function update_task_list(){
    // Grab the latest task list.
    $.getJSON("/get_task_list", function(data){
        var task_info = "";

        $('#stop-frequency-select').children().remove();

        added_decoders = false;

        for (_task in data){
            // Append the current task to the task list text.
            task_info += "SDR #" + _task + ": " + data[_task]["task"] + "    ";
            if(data[_task]["freq"] > 0.0){
                $('#stop-frequency-select')
                    .append($("<option></option>")
                    .attr("value",data[_task]["freq"])
                    .text( (parseFloat( data[_task]["freq"] )/1e6).toFixed(3)));

                added_decoders = true;
            }
        }

        if(added_decoders == false){
            $('#stop-frequency-select')
                    .append($("<option></option>")
                    .attr("value","0")
                    .text("No Decoders"));
        }
        
        // Update page with latest task.
        $('#task_status').text(task_info);
    });
}


function verify_password(){
    // Attempt to verify a password provided by a user, and update the password verify indication if its ok.

    // Grab the password
    _api_password = $('#password-input').val();

    // Do the request
    $.post(
        "/check_password", 
        {"password": _api_password},
        function(data){
            // If OK, update the header to indicate the password was OK.
            $("#password-header").html("<h2>Password OK!</h2>");
        }
    ).fail(function(xhr, status, error){
        // Otherwise, we probably got a 403 error (forbidden) which indicates the password was bad.
        if(error == "FORBIDDEN"){
            $("#password-header").html("<h2>Incorrect Password</h2>");
        }
    });
}

function disable_scanner(){
    // Disable the scanner.

    // Re-verify the password. This will occur async, so wont stop the main request from going ahead,
    // but will at least present an error for the user.
    verify_password();

    // Grab the password
    _api_password = $('#password-input').val();

    // Do the request
    $.post(
        "/disable_scanner", 
        {"password": _api_password},
        function(data){
            console.log(data);
            // Need to figure out where to put this data..
            alert("Scanner disable request received - please wait until SDR is shown as Not Tasked before issuing further requests.")
        }
    ).fail(function(xhr, status, error){
        console.log(error);
        // Otherwise, we probably got a 403 error (forbidden) which indicates the password was bad.
        if(error == "FORBIDDEN"){
            $("#password-header").html("<h2>Incorrect Password</h2>");
        } else if (error == "NOT FOUND"){
            // Scanner isn't running. Don't do anything.
            alert("Scanner not running!")
        } else if (error == "INTERNAL SERVER ERROR"){
            // Scanner might not have started up yet.
            alert("Scanner not initialised... (try again!)")
        }
    });
}

function enable_scanner(){
    // Enable the scanner.

    // Re-verify the password. This will occur async, so wont stop the main request from going ahead,
    // but will at least present an error for the user.
    verify_password();

    // Grab the password
    _api_password = $('#password-input').val();

    // Do the request
    $.post(
        "/enable_scanner", 
        {"password": _api_password},
        function(data){
            console.log(data);
            // Need to figure out where to put this data..
        }
    ).fail(function(xhr, status, error){
        console.log(error);
        // Otherwise, we probably got a 403 error (forbidden) which indicates the password was bad.
        if(error == "FORBIDDEN"){
            $("#password-header").html("<h2>Incorrect Password</h2>");
        }
    });
}

function stop_decoder(){
    // Stop the decoder on the requested frequency

    // Re-verify the password. This will occur async, so wont stop the main request from going ahead,
    // but will at least present an error for the user.
    verify_password();

    // Grab the password
    _api_password = $('#password-input').val();

    // Grab the selected frequency
    _decoder = $('#stop-frequency-select').val();

    // Do the request
    $.post(
        "/stop_decoder", 
        {password: _api_password, freq: _decoder},
        function(data){
            console.log(data);
            // Need to figure out where to put this data..
        }
    ).fail(function(xhr, status, error){
        console.log(error);
        // Otherwise, we probably got a 403 error (forbidden) which indicates the password was bad.
        if(error == "FORBIDDEN"){
            $("#password-header").html("<h2>Incorrect Password</h2>");
        } else if (error == "NOT FOUND"){
            // Scanner isn't running. Don't do anything.
            alert("Decoder on supplied frequency not running!")
        }
    });
}

function start_decoder(){
    // Start a decoder on the requested frequency

    // Re-verify the password. This will occur async, so wont stop the main request from going ahead,
    // but will at least present an error for the user.
    verify_password();

    // Grab the password
    _api_password = $('#password-input').val();

    // Grab the selected frequency
    _freq = $('#frequency-input').val();

    // Grab the selected type
    _type = $('#sonde-type-select').val();

    // Parse to a floar.
    _freq_float = parseFloat(_freq);
    if(_freq_float > autorx_config["max_freq"]){
        alert("Supplied frequency above maximum (" + autorx_config["max_freq"] + " MHz)");
        return;
    }
    if(_freq_float < autorx_config["min_freq"]){
        alert("Supplied frequency below minimum (" + autorx_config["min_freq"] + " MHz)");
        return;
    }

    _freq_hz = (_freq_float*1e6).toFixed(1);

    // Do the request
    $.post(
        "/start_decoder", 
        {password: _api_password, freq: _freq_hz, type: _type},
        function(data){
            alert("Added requested decoder to results queue.")
        }
    ).fail(function(xhr, status, error){
        console.log(error);
        // Otherwise, we probably got a 403 error (forbidden) which indicates the password was bad.
        if(error == "FORBIDDEN"){
            $("#password-header").html("<h2>Incorrect Password</h2>");
        }
    });
}