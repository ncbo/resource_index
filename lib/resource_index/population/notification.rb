require 'pony'

module RI::Population::Notification
  def success_email
    return unless @mail_recipients

    options = {
      subject: "[RI] completed #{@res.acronym}",
      body: "Population finished at #{Time.now.to_s}",
      recipients: @mail_recipients
    }

    notify(options)
  end

  def error_email(error)
    return unless @mail_recipients

    options = {
      subject: "[RI] error #{@res.acronym}",
      body: "Population error at #{Time.now.to_s}<br><br>#{error.message}<br><ul><li>#{error.backtrace.join("</li><li>")}</li></ul>",
      recipients: @mail_recipients
    }

    notify(options)
  end

  private

  def notify(options = {})
    headers    = { 'Content-Type' => 'text/html' }
    sender     = options[:sender] || "admin@bioontology.org"
    recipients = options[:recipients]
    raise ArgumentError, "Recipient needs to be provided in options[:recipients]" if !recipients || recipients.empty?

    Pony.mail({
      to: recipients,
      from: sender,
      subject: options[:subject],
      body: options[:body],
      headers: headers,
      via: :smtp,
      via_options: mail_options
    })
  end

  def mail_options
    options = {
      address: @smtp_host,
      port:    @smtp_port,
      domain:  @smtp_domain # the HELO domain provided by the client to the server
    }

    if @smtp_auth_type && @smtp_auth_type != :none
      options.merge({
        user_name:      @smtp_user,
        password:       @smtp_password,
        authentication: @smtp_auth_type
      })
    end

    return options
  end
end